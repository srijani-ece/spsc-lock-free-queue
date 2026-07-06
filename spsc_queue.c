#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct {
    uint8_t *buffer;
    size_t capacity_mask;
    _Atomic size_t head; // Written by Producer
    _Atomic size_t tail; // Written by Consumer
} spsc_queue_t;

// Initialize queue (capacity must be a power of 2)
void spsc_queue_init(spsc_queue_t *q, uint8_t *buf, size_t capacity) {
    q->buffer = buf;
    q->capacity_mask = capacity - 1;
    atomic_init(&q->head, 0);
    atomic_init(&q->tail, 0);
}

// Producer: Push data
bool spsc_queue_push(spsc_queue_t *q, uint8_t data) {
    size_t head = atomic_load_explicit(&q->head, memory_order_relaxed);
    size_t next_head = (head + 1) & q->capacity_mask;

    // Check if full (if next_head reaches tail)
    if (next_head == atomic_load_explicit(&q->tail, memory_order_acquire)) {
        return false;
    }

    q->buffer[head] = data;
    // Release ensures the data write completes before the head index updates
    atomic_store_explicit(&q->head, next_head, memory_order_release);
    return true;
}

// Consumer: Pop data
bool spsc_queue_pop(spsc_queue_t *q, uint8_t *data) {
    size_t tail = atomic_load_explicit(&q->tail, memory_order_relaxed);

    // Check if empty (if tail catches up to head)
    if (tail == atomic_load_explicit(&q->head, memory_order_acquire)) {
        return false;
    }

    *data = q->buffer[tail];
    // Release ensures the data read completes before the tail index updates
    atomic_store_explicit(&q->tail, (tail + 1) & q->capacity_mask, memory_order_release);
    return true;
}