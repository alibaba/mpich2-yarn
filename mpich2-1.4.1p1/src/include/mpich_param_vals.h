/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 *  (C) 2011 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */
/* automatically generated
 *   by:   ./maint/genparams
 *   on:   Thu Sep  1 13:55:43 2011
 *   from: src/util/param/params.yml (md5sum 6d969dfaee560cd15ba5b677edcb1867)
 *
 * DO NOT EDIT!!!
 */

#if !defined(MPICH_PARAM_VALS_H_INCLUDED)
#define MPICH_PARAM_VALS_H_INCLUDED

/* parameter categories */
enum MPIR_Param_category_id_t {
    MPIR_PARAM_CATEGORY_ID_collective,
    MPIR_PARAM_CATEGORY_ID_communicator,
    MPIR_PARAM_CATEGORY_ID_pt2pt,
    MPIR_PARAM_CATEGORY_ID_intranode,
    MPIR_PARAM_CATEGORY_ID_developer,
    MPIR_PARAM_CATEGORY_ID_memory,
    MPIR_PARAM_CATEGORY_ID_error_handling,
    MPIR_PARAM_CATEGORY_ID_debugger,
    MPIR_PARAM_CATEGORY_ID_checkpointing,
    MPIR_PARAM_CATEGORY_ID_fault_tolerance,
    MPIR_PARAM_CATEGORY_ID_threads,
    MPIR_PARAM_CATEGORY_ID_nemesis,
    MPIR_PARAM_CATEGORY_ID_sockets,
    MPIR_PARAM_NUM_CATEGORIES
};

struct MPIR_Param_category_t {
    const enum MPIR_Param_category_id_t id;
    const char *name;
    const char *description;
};

/* array of category info for runtime usage */
extern struct MPIR_Param_category_t MPIR_Param_categories[MPIR_PARAM_NUM_CATEGORIES];

/* parameter values */
enum MPIR_Param_id_t {
    MPIR_PARAM_ID_ALLTOALL_SHORT_MSG_SIZE,
    MPIR_PARAM_ID_ALLTOALL_MEDIUM_MSG_SIZE,
    MPIR_PARAM_ID_ALLTOALL_THROTTLE,
    MPIR_PARAM_ID_REDSCAT_COMMUTATIVE_LONG_MSG_SIZE,
    MPIR_PARAM_ID_BCAST_MIN_PROCS,
    MPIR_PARAM_ID_BCAST_SHORT_MSG_SIZE,
    MPIR_PARAM_ID_BCAST_LONG_MSG_SIZE,
    MPIR_PARAM_ID_ALLGATHER_SHORT_MSG_SIZE,
    MPIR_PARAM_ID_ALLGATHER_LONG_MSG_SIZE,
    MPIR_PARAM_ID_REDUCE_SHORT_MSG_SIZE,
    MPIR_PARAM_ID_ALLREDUCE_SHORT_MSG_SIZE,
    MPIR_PARAM_ID_GATHER_VSMALL_MSG_SIZE,
    MPIR_PARAM_ID_GATHER_INTER_SHORT_MSG_SIZE,
    MPIR_PARAM_ID_GATHERV_INTER_SSEND_MIN_PROCS,
    MPIR_PARAM_ID_SCATTER_INTER_SHORT_MSG_SIZE,
    MPIR_PARAM_ID_ALLGATHERV_PIPELINE_MSG_SIZE,
    MPIR_PARAM_ID_COMM_SPLIT_USE_QSORT,
    MPIR_PARAM_ID_NOLOCAL,
    MPIR_PARAM_ID_ODD_EVEN_CLIQUES,
    MPIR_PARAM_ID_MEMDUMP,
    MPIR_PARAM_ID_PROCTABLE_SIZE,
    MPIR_PARAM_ID_PROCTABLE_PRINT,
    MPIR_PARAM_ID_ERROR_CHECKING,
    MPIR_PARAM_ID_PRINT_ERROR_STACK,
    MPIR_PARAM_ID_CHOP_ERROR_STACK,
    MPIR_PARAM_ID_NEM_LMT_DMA_THRESHOLD,
    MPIR_PARAM_ID_NEMESIS_NETMOD,
    MPIR_PARAM_ID_INTERFACE_HOSTNAME,
    MPIR_PARAM_ID_NETWORK_IFACE,
    MPIR_PARAM_ID_HOST_LOOKUP_RETRIES,
    MPIR_PARAM_ID_DEBUG_HOLD,
    MPIR_PARAM_ID_ENABLE_CKPOINT,
    MPIR_PARAM_ID_ENABLE_COLL_FT_RET,
    MPIR_PARAM_ID_ABORT_ON_LEAKED_HANDLES,
    MPIR_PARAM_ID_PORT_RANGE,
    MPIR_PARAM_NUM_PARAMS
};

/* initializes parameter values from the environment */
int MPIR_Param_init_params(void);

enum MPIR_Param_type_t {
    MPIR_PARAM_TYPE_INVALID = 0,
    MPIR_PARAM_TYPE_INT,
    MPIR_PARAM_TYPE_DOUBLE,
    MPIR_PARAM_TYPE_BOOLEAN,
    MPIR_PARAM_TYPE_STRING,
    MPIR_PARAM_TYPE_RANGE
};

typedef struct MPIR_Param_param_range_val {
    int low;
    int high;
} MPIR_Param_param_range_val_t;

/* used to represent default values */
struct MPIR_Param_param_default_val_t {
    const enum MPIR_Param_type_t type;

    /* not a union b/c of initialization portability issues */
    const int i_val; /* also used for booleans */
    const double d_val;
    const char *s_val;
    const MPIR_Param_param_range_val_t r_val;
};

struct MPIR_Param_t {
    const enum MPIR_Param_id_t id;
    const char *name;
    const char *description;
    const struct MPIR_Param_param_default_val_t default_val;
    /* TODO other fields here */
};

/* array of parameter info for runtime usage */
extern struct MPIR_Param_t MPIR_Param_params[MPIR_PARAM_NUM_PARAMS];

/* extern declarations for each parameter
 * (definitions in src/util/param/param_vals.c) */
extern int MPIR_PARAM_ALLTOALL_SHORT_MSG_SIZE;
extern int MPIR_PARAM_ALLTOALL_MEDIUM_MSG_SIZE;
extern int MPIR_PARAM_ALLTOALL_THROTTLE;
extern int MPIR_PARAM_REDSCAT_COMMUTATIVE_LONG_MSG_SIZE;
extern int MPIR_PARAM_BCAST_MIN_PROCS;
extern int MPIR_PARAM_BCAST_SHORT_MSG_SIZE;
extern int MPIR_PARAM_BCAST_LONG_MSG_SIZE;
extern int MPIR_PARAM_ALLGATHER_SHORT_MSG_SIZE;
extern int MPIR_PARAM_ALLGATHER_LONG_MSG_SIZE;
extern int MPIR_PARAM_REDUCE_SHORT_MSG_SIZE;
extern int MPIR_PARAM_ALLREDUCE_SHORT_MSG_SIZE;
extern int MPIR_PARAM_GATHER_VSMALL_MSG_SIZE;
extern int MPIR_PARAM_GATHER_INTER_SHORT_MSG_SIZE;
extern int MPIR_PARAM_GATHERV_INTER_SSEND_MIN_PROCS;
extern int MPIR_PARAM_SCATTER_INTER_SHORT_MSG_SIZE;
extern int MPIR_PARAM_ALLGATHERV_PIPELINE_MSG_SIZE;
extern int MPIR_PARAM_COMM_SPLIT_USE_QSORT;
extern int MPIR_PARAM_NOLOCAL;
extern int MPIR_PARAM_ODD_EVEN_CLIQUES;
extern int MPIR_PARAM_MEMDUMP;
extern int MPIR_PARAM_PROCTABLE_SIZE;
extern int MPIR_PARAM_PROCTABLE_PRINT;
extern int MPIR_PARAM_ERROR_CHECKING;
extern int MPIR_PARAM_PRINT_ERROR_STACK;
extern int MPIR_PARAM_CHOP_ERROR_STACK;
extern int MPIR_PARAM_NEM_LMT_DMA_THRESHOLD;
extern const char * MPIR_PARAM_NEMESIS_NETMOD;
extern const char * MPIR_PARAM_INTERFACE_HOSTNAME;
extern const char * MPIR_PARAM_NETWORK_IFACE;
extern int MPIR_PARAM_HOST_LOOKUP_RETRIES;
extern int MPIR_PARAM_DEBUG_HOLD;
extern int MPIR_PARAM_ENABLE_CKPOINT;
extern int MPIR_PARAM_ENABLE_COLL_FT_RET;
extern int MPIR_PARAM_ABORT_ON_LEAKED_HANDLES;
extern MPIR_Param_param_range_val_t MPIR_PARAM_PORT_RANGE;

/* TODO: this should be defined elsewhere */
#define MPIR_Param_assert MPIU_Assert

/* helper macros for safely getting the default value of a parameter */
#define MPIR_PARAM_GET_DEFAULT_INT(p_suffix_,out_ptr_)                                               \
    do {                                                                                               \
        MPIR_Param_assert(MPIR_PARAM_TYPE_INT == MPIR_Param_params[MPIR_PARAM_ID_##p_suffix_].default_val.type); \
        *(out_ptr_) = MPIR_Param_params[MPIR_PARAM_ID_##p_suffix_].default_val.i_val;                      \
    } while (0)
#define MPIR_PARAM_GET_DEFAULT_DOUBLE(p_suffix_,out_ptr_)                                               \
    do {                                                                                               \
        MPIR_Param_assert(MPIR_PARAM_TYPE_DOUBLE == MPIR_Param_params[MPIR_PARAM_ID_##p_suffix_].default_val.type); \
        *(out_ptr_) = MPIR_Param_params[MPIR_PARAM_ID_##p_suffix_].default_val.d_val;                      \
    } while (0)
#define MPIR_PARAM_GET_DEFAULT_BOOLEAN(p_suffix_,out_ptr_)                                               \
    do {                                                                                               \
        MPIR_Param_assert(MPIR_PARAM_TYPE_BOOLEAN == MPIR_Param_params[MPIR_PARAM_ID_##p_suffix_].default_val.type); \
        *(out_ptr_) = MPIR_Param_params[MPIR_PARAM_ID_##p_suffix_].default_val.i_val;                      \
    } while (0)
#define MPIR_PARAM_GET_DEFAULT_STRING(p_suffix_,out_ptr_)                                               \
    do {                                                                                               \
        MPIR_Param_assert(MPIR_PARAM_TYPE_STRING == MPIR_Param_params[MPIR_PARAM_ID_##p_suffix_].default_val.type); \
        *(out_ptr_) = MPIR_Param_params[MPIR_PARAM_ID_##p_suffix_].default_val.s_val;                      \
    } while (0)
#define MPIR_PARAM_GET_DEFAULT_RANGE(p_suffix_,out_ptr_)                                               \
    do {                                                                                               \
        MPIR_Param_assert(MPIR_PARAM_TYPE_RANGE == MPIR_Param_params[MPIR_PARAM_ID_##p_suffix_].default_val.type); \
        *(out_ptr_) = MPIR_Param_params[MPIR_PARAM_ID_##p_suffix_].default_val.r_val;                      \
    } while (0)

#endif /* MPICH_PARAM_VALS_H_INCLUDED */
