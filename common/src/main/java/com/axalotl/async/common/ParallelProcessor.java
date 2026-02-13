package com.axalotl.async.common;

import com.axalotl.async.common.config.AsyncConfig;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.*;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.*;
import lombok.Getter;
import lombok.Setter;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.EntityType;
import net.minecraft.world.entity.item.FallingBlockEntity;
import net.minecraft.world.entity.monster.Shulker;
import net.minecraft.world.entity.projectile.Projectile;
import net.minecraft.world.entity.vehicle.AbstractMinecart;
import net.minecraft.world.entity.vehicle.Boat;
import net.minecraft.world.level.NaturalSpawner;
import net.minecraft.world.level.chunk.LevelChunk;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.invoke.*;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class ParallelProcessor {
    public static final Logger LOGGER = LogManager.getLogger(ParallelProcessor.class);

    @Getter
    @Setter
    private static MinecraftServer server;

    @Contended
    @sun.misc.Contended
    public static class PaddedLongAdder extends LongAdder {
    }
    
    public static final PaddedLongAdder currentEntities = new PaddedLongAdder();
    public static final PaddedLongAdder totalEntitiesProcessed = new PaddedLongAdder();
    public static final PaddedLongAdder asyncTicksExecuted = new PaddedLongAdder();
    
    private static final AtomicInteger threadPoolID = new AtomicInteger();
    
    private static final int RING_BUFFER_SIZE = 4096;
    private static Disruptor<EntityEvent> disruptor;
    private static RingBuffer<EntityEvent> ringBuffer;
    
    public static final class EntityEvent {
        ServerLevel level;
        Entity entity;
        int eventType;
        
        void clear() {
            level = null;
            entity = null;
        }
    }
    
    private static final EventFactory<EntityEvent> EVENT_FACTORY = EntityEvent::new;
    
    private static final class EntityEventHandler implements EventHandler<EntityEvent> {
        @Override
        public void onEvent(EntityEvent event, long sequence, boolean endOfBatch) {
            try {
                switch (event.eventType) {
                    case 0 -> performAsyncEntityTick(event.level, event.entity);
                    case 1 -> NaturalSpawner.spawnForChunk(event.level, 
                        (LevelChunk) event.entity, null, true, true, true);
                    case 2 -> event.entity.checkDespawn();
                }
            } catch (Exception e) {
                LOGGER.error("Error processing entity event", e);
            } finally {
                event.clear();
            }
        }
    }
    
    private static final Long2LongOpenHashMap portalTickSyncMap = new Long2LongOpenHashMap(64, 0.75f);
    
    private static final LongOpenHashSet blacklistedEntities = new LongOpenHashSet(64, 0.75f);
    
    private static final Reference2BooleanOpenHashMap<EntityType<?>> syncEntityCache = 
        new Reference2BooleanOpenHashMap<>(256, 0.75f);
    
    private static final int POOL_SIZE = 512;
    
    private static final ConcurrentLinkedQueue<BatchRecord> batchRecordPool = new ConcurrentLinkedQueue<>();
    
    private static final ThreadLocal<BatchRecord[]> batchArrays = 
        ThreadLocal.withInitial(() -> new BatchRecord[128]);
    private static final ThreadLocal<int[]> batchCounts = 
        ThreadLocal.withInitial(() -> new int[1]);
    
    public static final class BatchRecord {
        ServerLevel level;
        Entity entity;
        
        void set(ServerLevel level, Entity entity) {
            this.level = level;
            this.entity = entity;
        }
        
        void clear() {
            level = null;
            entity = null;
        }
    }
    
    private static final VarHandle BATCH_COUNT_HANDLE;
    
    static {
        try {
            BATCH_COUNT_HANDLE = MethodHandles.arrayElementVarHandle(int[].class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    
    private static final ClassValue<Boolean> blockedClassCache = new ClassValue<>() {
        @Override
        protected Boolean computeValue(Class<?> type) {
            return type == FallingBlockEntity.class 
                || type == Shulker.class 
                || type == Boat.class;
        }
    };
    
    private static final ClassValue<Boolean> projectileCache = new ClassValue<>() {
        @Override
        protected Boolean computeValue(Class<?> type) {
            return Projectile.class.isAssignableFrom(type);
        }
    };
    
    private static final ClassValue<Boolean> minecartCache = new ClassValue<>() {
        @Override
        protected Boolean computeValue(Class<?> type) {
            return AbstractMinecart.class.isAssignableFrom(type);
        }
    };
    
    private static final ClassValue<Boolean> playerCache = new ClassValue<>() {
        @Override
        protected Boolean computeValue(Class<?> type) {
            return ServerPlayer.class.isAssignableFrom(type);
        }
    };
    
    public static final Set<Class<?>> BLOCKED_ENTITIES = Set.of(
        FallingBlockEntity.class,
        Shulker.class,
        Boat.class
    );
    
    private static final Long2ObjectOpenHashMap<WeakReference<Thread>> threadMap = 
        new Long2ObjectOpenHashMap<>(32, 0.75f);
    
    private static volatile boolean isShuttingDown = false;
    private static final Object ENTITY_ADD_LOCK = new Object();
    
    private static final int MAX_FUTURES = 2048;
    private static final CompletableFuture<?>[] futuresBuffer = new CompletableFuture[MAX_FUTURES];
    
    public static ForkJoinPool tickPool;
    
    public static final ConcurrentLinkedQueue<CompletableFuture<?>> taskQueue = new ConcurrentLinkedQueue<>();

    public static void setupThreadPool(int parallelism, Class<?> asyncClass) {
        isShuttingDown = false;
        
        for (int i = 0; i < POOL_SIZE; i++) {
            batchRecordPool.offer(new BatchRecord());
        }
        
        disruptor = new Disruptor<>(
            EVENT_FACTORY,
            RING_BUFFER_SIZE,
            DaemonThreadFactory.INSTANCE,
            ProducerType.MULTI,
            new BlockingWaitStrategy() 
        );
        
        disruptor.handleEventsWith(new EntityEventHandler());
        ringBuffer = disruptor.start();
        
        ForkJoinPool.ForkJoinWorkerThreadFactory factory = pool -> {
            ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
            worker.setName("Async-Tick-" + threadPoolID.getAndIncrement());
            worker.setDaemon(true);
            worker.setPriority(Thread.NORM_PRIORITY - 1);
            worker.setContextClassLoader(asyncClass.getClassLoader());
            threadMap.put(worker.threadId(), new WeakReference<>(worker));
            return worker;
        };
        
        tickPool = new ForkJoinPool(parallelism, factory, (t, e) ->
            LOGGER.error("Uncaught exception in {}: {}", t.getName(), e), true);
        
        LOGGER.info("EXTREME MODE: Disruptor + fastutil + VarHandle initialized");
    }
    
    @SuppressWarnings("deprecation")
    private static long uuidToLong(UUID uuid) {
        return uuid.getMostSignificantBits() ^ uuid.getLeastSignificantBits();
    }
    
    private static long entityToKey(Entity entity) {
        return uuidToLong(entity.getUUID());
    }
    
    public static void callEntityTick(ServerLevel world, Entity entity) {
        if (isShuttingDown) {
            tickSynchronously(world, entity);
            return;
        }
        
        if (entity.level().isClientSide()) {
            tickSynchronously(world, entity);
            return;
        }
        
        if (AsyncConfig.disabled.getValue()) {
            tickSynchronously(world, entity);
            return;
        }
        
        Class<?> entityClass = entity.getClass();
        if (playerCache.get(entityClass) 
            || projectileCache.get(entityClass) 
            || minecartCache.get(entityClass)
            || blockedClassCache.get(entityClass)) {
            tickSynchronously(world, entity);
            return;
        }
        
        long entityKey = entityToKey(entity);
        if (blacklistedEntities.contains(entityKey)) {
            tickSynchronously(world, entity);
            return;
        }
        
        EntityType<?> type = entity.getType();
        if (syncEntityCache.computeIfAbsent(type, t -> 
            AsyncConfig.isEntitySynchronized(EntityType.getKey(t)))) {
            tickSynchronously(world, entity);
            return;
        }
        
        long portalExpiry = portalTickSyncMap.get(entityKey);
        if (portalExpiry > 0) {
            long currentTime = System.currentTimeMillis();
            if (currentTime < portalExpiry) {
                tickSynchronously(world, entity);
                return;
            } else {
                portalTickSyncMap.remove(entityKey);
            }
        }
        
        if (entity.isInsidePortal) {
            portalTickSyncMap.put(entityKey, System.currentTimeMillis() + 2000);
            tickSynchronously(world, entity);
            return;
        }
        
        long sequence = ringBuffer.tryNext();
        if (sequence >= 0) {
            EntityEvent event = ringBuffer.get(sequence);
            event.level = world;
            event.entity = entity;
            event.eventType = 0;
            ringBuffer.publish(sequence);
            return;
        }
        
        addToBatch(world, entity);
    }
    
    private static void addToBatch(ServerLevel world, Entity entity) {
        BatchRecord[] array = batchArrays.get();
        int[] countArray = batchCounts.get();
        
        int index = (int) BATCH_COUNT_HANDLE.getOpaque(countArray, 0);
        
        if (index >= array.length) {
            flushBatch(array, countArray, index);
            index = 0;
        }
        
        BatchRecord record = batchRecordPool.poll();
        if (record == null) {
            record = new BatchRecord();
        }
        record.set(world, entity);
        array[index] = record;
        
        BATCH_COUNT_HANDLE.setOpaque(countArray, 0, index + 1);
    }
    
    private static void flushBatch(BatchRecord[] array, int[] countArray, int count) {
        if (count == 0) return;
        
        tickPool.execute(() -> {
            for (int i = 0; i < count; i++) {
                BatchRecord record = array[i];
                if (record != null) {
                    performAsyncEntityTick(record.level, record.entity);
                    record.clear();
                    batchRecordPool.offer(record);
                    array[i] = null;
                }
            }
        });
        
        BATCH_COUNT_HANDLE.setOpaque(countArray, 0, 0);
    }
    
    public static boolean shouldTickSynchronously(Entity entity) {
        if (entity.level().isClientSide()) return true;
        if (AsyncConfig.disabled.getValue()) return true;
        
        Class<?> entityClass = entity.getClass();
        if (playerCache.get(entityClass)) return true;
        if (projectileCache.get(entityClass)) return true;
        if (minecartCache.get(entityClass)) return true;
        if (blockedClassCache.get(entityClass)) return true;
        
        long entityKey = entityToKey(entity);
        if (blacklistedEntities.contains(entityKey)) return true;
        
        if (syncEntityCache.computeIfAbsent(entity.getType(), t -> 
            AsyncConfig.isEntitySynchronized(EntityType.getKey(t)))) {
            return true;
        }
        
        long portalExpiry = portalTickSyncMap.get(entityKey);
        if (portalExpiry > 0) {
            if (System.currentTimeMillis() < portalExpiry) {
                return true;
            }
            portalTickSyncMap.remove(entityKey);
        }
        
        if (entity.isInsidePortal) {
            portalTickSyncMap.put(entityKey, System.currentTimeMillis() + 2000);
            return true;
        }
        
        return false;
    }
    
    private static void tickSynchronously(ServerLevel world, Entity entity) {
        try {
            world.tickNonPassenger(entity);
        } catch (Exception e) {
            logEntityError("Sync tick error", entity, e);
        }
    }
    
    private static void performAsyncEntityTick(ServerLevel world, Entity entity) {
        currentEntities.increment();
        try {
            world.tickNonPassenger(entity);
            asyncTicksExecuted.increment();
        } catch (Exception e) {
            LOGGER.warn("Async tick failed for {}, blacklisting", entity.getType());
            blacklistedEntities.add(entityToKey(entity));
        } finally {
            currentEntities.decrement();
        }
    }
    
    public static void asyncSpawnForChunk(
            ServerLevel level,
            LevelChunk chunk,
            NaturalSpawner.SpawnState spawnState,
            boolean spawnAnimals, boolean spawnMonsters, boolean rareSpawn)
    {
        if (!chunk.loaded) return;
        
        if (isShuttingDown || AsyncConfig.disabled.getValue() || !AsyncConfig.enableAsyncSpawn.getValue()) {
            NaturalSpawner.spawnForChunk(level, chunk, spawnState, spawnAnimals, spawnMonsters, rareSpawn);
            return;
        }
        
        if (!spawnAnimals && !spawnMonsters && !rareSpawn) return;
        
        long sequence = ringBuffer.tryNext();
        if (sequence >= 0) {
            EntityEvent event = ringBuffer.get(sequence);
            event.level = level;
            event.entity = (Entity) chunk; 
            event.eventType = 1;
            ringBuffer.publish(sequence);
            return;
        }
        
        tickPool.execute(() -> 
            NaturalSpawner.spawnForChunk(level, chunk, spawnState, spawnAnimals, spawnMonsters, rareSpawn));
    }
    
    public static void asyncDespawn(Entity entity) {
        if (isShuttingDown || AsyncConfig.disabled.getValue() || !AsyncConfig.enableAsyncSpawn.getValue()) {
            entity.checkDespawn();
            return;
        }
        
        long sequence = ringBuffer.tryNext();
        if (sequence >= 0) {
            EntityEvent event = ringBuffer.get(sequence);
            event.level = null;
            event.entity = entity;
            event.eventType = 2;
            ringBuffer.publish(sequence);
            return;
        }
        
        tickPool.execute(entity::checkDespawn);
    }
    
    public static void postEntityTick() {
        BatchRecord[] array = batchArrays.get();
        int[] countArray = batchCounts.get();
        int count = (int) BATCH_COUNT_HANDLE.getOpaque(countArray, 0);
        if (count > 0) {
            flushBatch(array, countArray, count);
        }
        
        if (AsyncConfig.disabled.getValue()) return;
        
        int futureCount = 0;
        CompletableFuture<?> f;
        while ((f = taskQueue.poll()) != null && futureCount < MAX_FUTURES) {
            futuresBuffer[futureCount++] = f;
        }
        
        if (futureCount == 0) return;
        
        CompletableFuture<Void> allOf = CompletableFuture.allOf(
            Arrays.copyOf(futuresBuffer, futureCount));
        
        long deadline = System.nanoTime() + 45_000_000L;
        
        while (!allOf.isDone() && System.nanoTime() < deadline) {
            boolean didWork = false;
            for (ServerLevel world : server.getAllLevels()) {
                didWork |= world.getChunkSource().pollTask();
            }
            if (!didWork) {
                Thread.onSpinWait();
            }
        }
        
        if (!allOf.isDone()) {
            LOGGER.warn("Tick timeout!");
        }
        
        Arrays.fill(futuresBuffer, 0, futureCount, null);
    }
    
    public static void addTask(CompletableFuture<?> future) {
        taskQueue.add(future);
    }
    
    public static Object getEntityAddLock() {
        return ENTITY_ADD_LOCK;
    }
    
    public static void stop() {
        isShuttingDown = true;
        
        BatchRecord[] array = batchArrays.get();
        int[] countArray = batchCounts.get();
        int count = (int) BATCH_COUNT_HANDLE.getOpaque(countArray, 0);
        if (count > 0) {
            for (int i = 0; i < count; i++) {
                BatchRecord record = array[i];
                if (record != null) {
                    tickSynchronously(record.level, record.entity);
                    record.clear();
                }
            }
        }
        
        if (disruptor != null) {
            try {
                disruptor.shutdown(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                disruptor.halt();
            }
        }
        
        if (tickPool != null) {
            tickPool.shutdown();
            try {
                if (!tickPool.awaitTermination(5, TimeUnit.SECONDS)) {
                    tickPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                tickPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        blacklistedEntities.clear();
        portalTickSyncMap.clear();
        syncEntityCache.clear();
        batchRecordPool.clear();
        threadMap.clear();
        taskQueue.clear();
        
        batchArrays.remove();
        batchCounts.remove();
    }
    
    public static int getCurrentEntityCount() {
        return currentEntities.intValue();
    }
    
    public static long getTotalProcessed() {
        return totalEntitiesProcessed.longValue();
    }
    
    public static long getAsyncTicksExecuted() {
        return asyncTicksExecuted.longValue();
    }
    
    private static void logEntityError(String msg, Entity entity, Throwable e) {
        try {
            LOGGER.error("{}: {} ({})", msg, entity.getType(), entity.getUUID(), e);
        } catch (Exception ex) {
            LOGGER.error(msg, e);
        }
    }
    
    private enum DaemonThreadFactory implements ThreadFactory {
        INSTANCE;
        
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "Disruptor-Handler");
            t.setDaemon(true);
            t.setPriority(Thread.NORM_PRIORITY - 1);
            return t;
        }
    }
}
