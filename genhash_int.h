/**
 * \private
 */
struct genhash_entry_t {
    /** The key for this entry */
    void *key;
    /** The value for this entry */
    void *value;
    /** Pointer to the next entry */
    struct genhash_entry_t *next;
};

struct _genhash {
    size_t size;
    struct hash_ops ops;
    struct genhash_entry_t *buckets[];
};
