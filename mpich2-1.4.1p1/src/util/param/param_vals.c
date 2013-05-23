/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 *  (C) 2011 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */
/* automatically generated
 *   by:   ./maint/genparams
 *   at:   Thu Sep  1 13:55:43 2011
 *   from: src/util/param/params.yml (md5sum 6d969dfaee560cd15ba5b677edcb1867)
 *
 * DO NOT EDIT!!!
 */

#include "mpiimpl.h"

/* array of category info for runtime usage */
struct MPIR_Param_category_t MPIR_Param_categories[MPIR_PARAM_NUM_CATEGORIES] = {
    { MPIR_PARAM_CATEGORY_ID_collective,
      "collective",
      "parameters that control collective communication behavior" },
    { MPIR_PARAM_CATEGORY_ID_communicator,
      "communicator",
      "parameters that control communicator construction and operation" },
    { MPIR_PARAM_CATEGORY_ID_pt2pt,
      "pt2pt",
      "parameters that control point-to-point communication behavior" },
    { MPIR_PARAM_CATEGORY_ID_intranode,
      "intranode",
      "intranode communication parameters" },
    { MPIR_PARAM_CATEGORY_ID_developer,
      "developer",
      "useful for developers working on MPICH2 itself" },
    { MPIR_PARAM_CATEGORY_ID_memory,
      "memory",
      "affects memory allocation and usage, including MPI object handles" },
    { MPIR_PARAM_CATEGORY_ID_error_handling,
      "error_handling",
      "parameters that control error handling behavior (stack traces, aborts, etc)" },
    { MPIR_PARAM_CATEGORY_ID_debugger,
      "debugger",
      "parameters relevant to the \"MPIR\" debugger interface" },
    { MPIR_PARAM_CATEGORY_ID_checkpointing,
      "checkpointing",
      "parameters relevant to checkpointing" },
    { MPIR_PARAM_CATEGORY_ID_fault_tolerance,
      "fault_tolerance",
      "parameters that control fault tolerance behavior" },
    { MPIR_PARAM_CATEGORY_ID_threads,
      "threads",
      "multi-threading parameters" },
    { MPIR_PARAM_CATEGORY_ID_nemesis,
      "nemesis",
      "parameters that control behavior of the ch3:nemesis channel" },
    { MPIR_PARAM_CATEGORY_ID_sockets,
      "sockets",
      "control socket parameters" },
};

/* array of parameter info for runtime usage */
struct MPIR_Param_t MPIR_Param_params[MPIR_PARAM_NUM_PARAMS] = {
    { MPIR_PARAM_ID_ALLTOALL_SHORT_MSG_SIZE,
      "ALLTOALL_SHORT_MSG_SIZE",
      "the short message algorithm will be used if the per-destination message size (sendcount*size(sendtype)) is <= this value",
      { MPIR_PARAM_TYPE_INT, 256, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_ALLTOALL_MEDIUM_MSG_SIZE,
      "ALLTOALL_MEDIUM_MSG_SIZE",
      "the medium message algorithm will be used if the per-destination message size (sendcount*size(sendtype)) is <= this value and larger than ALLTOALL_SHORT_MSG_SIZE",
      { MPIR_PARAM_TYPE_INT, 32768, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_ALLTOALL_THROTTLE,
      "ALLTOALL_THROTTLE",
      "max no. of irecvs/isends posted at a time in some alltoall algorithms. Setting it to 0 causes all irecvs/isends to be posted at once.",
      { MPIR_PARAM_TYPE_INT, 4, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_REDSCAT_COMMUTATIVE_LONG_MSG_SIZE,
      "REDSCAT_COMMUTATIVE_LONG_MSG_SIZE",
      "the long message algorithm will be used if the operation is commutative and the send buffer size is >= this value (in bytes)",
      { MPIR_PARAM_TYPE_INT, 524288, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_BCAST_MIN_PROCS,
      "BCAST_MIN_PROCS",
      "the minimum number of processes in a communicator to use a non-binomial broadcast algorithm",
      { MPIR_PARAM_TYPE_INT, 8, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_BCAST_SHORT_MSG_SIZE,
      "BCAST_SHORT_MSG_SIZE",
      "the short message algorithm will be used if the send buffer size is < this value (in bytes)",
      { MPIR_PARAM_TYPE_INT, 12288, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_BCAST_LONG_MSG_SIZE,
      "BCAST_LONG_MSG_SIZE",
      "the long message algorithm will be used if the send buffer size is >= this value (in bytes)",
      { MPIR_PARAM_TYPE_INT, 524288, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_ALLGATHER_SHORT_MSG_SIZE,
      "ALLGATHER_SHORT_MSG_SIZE",
      "For MPI_Allgather and MPI_Allgatherv, the short message algorithm will be used if the send buffer size is < this value (in bytes).",
      { MPIR_PARAM_TYPE_INT, 81920, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_ALLGATHER_LONG_MSG_SIZE,
      "ALLGATHER_LONG_MSG_SIZE",
      "For MPI_Allgather and MPI_Allgatherv, the long message algorithm will be used if the send buffer size is >= this value (in bytes)",
      { MPIR_PARAM_TYPE_INT, 524288, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_REDUCE_SHORT_MSG_SIZE,
      "REDUCE_SHORT_MSG_SIZE",
      "the short message algorithm will be used if the send buffer size is <= this value (in bytes)",
      { MPIR_PARAM_TYPE_INT, 2048, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_ALLREDUCE_SHORT_MSG_SIZE,
      "ALLREDUCE_SHORT_MSG_SIZE",
      "the short message algorithm will be used if the send buffer size is <= this value (in bytes)",
      { MPIR_PARAM_TYPE_INT, 2048, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_GATHER_VSMALL_MSG_SIZE,
      "GATHER_VSMALL_MSG_SIZE",
      "use a temporary buffer for intracommunicator MPI_Gather if the send buffer size is < this value (in bytes)",
      { MPIR_PARAM_TYPE_INT, 1024, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_GATHER_INTER_SHORT_MSG_SIZE,
      "GATHER_INTER_SHORT_MSG_SIZE",
      "use the short message algorithm for intercommunicator MPI_Gather if the send buffer size is < this value (in bytes)",
      { MPIR_PARAM_TYPE_INT, 2048, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_GATHERV_INTER_SSEND_MIN_PROCS,
      "GATHERV_INTER_SSEND_MIN_PROCS",
      "Use Ssend (synchronous send) for intercommunicator MPI_Gatherv if the \"group B\" size is >= this value.  Specifying \"-1\" always avoids using Ssend.  For backwards compatibility, specifying \"0\" uses the default value.",
      { MPIR_PARAM_TYPE_INT, 32, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_SCATTER_INTER_SHORT_MSG_SIZE,
      "SCATTER_INTER_SHORT_MSG_SIZE",
      "use the short message algorithm for intercommunicator MPI_Scatter if the send buffer size is < this value (in bytes)",
      { MPIR_PARAM_TYPE_INT, 2048, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_ALLGATHERV_PIPELINE_MSG_SIZE,
      "ALLGATHERV_PIPELINE_MSG_SIZE",
      "The smallest message size that will be used for the pipelined, large-message, ring algorithm in the MPI_Allgatherv implementation.",
      { MPIR_PARAM_TYPE_INT, 32768, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_COMM_SPLIT_USE_QSORT,
      "COMM_SPLIT_USE_QSORT",
      "Use qsort(3) in the implementation of MPI_Comm_split instead of bubble sort.",
      { MPIR_PARAM_TYPE_BOOLEAN, 1, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_NOLOCAL,
      "NOLOCAL",
      "If true, force all processes to operate as though all processes are located on another node.  For example, this disables shared memory communication hierarchical collectives.",
      { MPIR_PARAM_TYPE_BOOLEAN, 0, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_ODD_EVEN_CLIQUES,
      "ODD_EVEN_CLIQUES",
      "If true, odd procs on a node are seen as local to each other, and even procs on a node are seen as local to each other.  Used for debugging on a single machine.",
      { MPIR_PARAM_TYPE_BOOLEAN, 0, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_MEMDUMP,
      "MEMDUMP",
      "If true, list any memory that was allocated by MPICH2 and that remains allocated when MPI_Finalize completes.",
      { MPIR_PARAM_TYPE_BOOLEAN, 1, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_PROCTABLE_SIZE,
      "PROCTABLE_SIZE",
      "Size of the \"MPIR\" debugger interface proctable (process table).",
      { MPIR_PARAM_TYPE_INT, 64, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_PROCTABLE_PRINT,
      "PROCTABLE_PRINT",
      "If true, dump the proctable entries at MPIR_WaitForDebugger-time. (currently compile-time disabled by \"#if 0\")",
      { MPIR_PARAM_TYPE_BOOLEAN, 1, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_ERROR_CHECKING,
      "ERROR_CHECKING",
      "If true, perform checks for errors, typically to verify valid inputs to MPI routines.  Only effective when MPICH2 is configured with --enable-error-checking=runtime .",
      { MPIR_PARAM_TYPE_BOOLEAN, 1, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_PRINT_ERROR_STACK,
      "PRINT_ERROR_STACK",
      "If true, print an error stack trace at error handling time.",
      { MPIR_PARAM_TYPE_BOOLEAN, 1, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_CHOP_ERROR_STACK,
      "CHOP_ERROR_STACK",
      "If >0, truncate error stack output lines this many characters wide.  If 0, do not truncate, and if <0 use a sensible default.",
      { MPIR_PARAM_TYPE_INT, 0, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_NEM_LMT_DMA_THRESHOLD,
      "NEM_LMT_DMA_THRESHOLD",
      "Messages larger than this size will use the \"dma\" (knem) intranode LMT implementation, if it is enabled and available.",
      { MPIR_PARAM_TYPE_INT, 2097152, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_NEMESIS_NETMOD,
      "NEMESIS_NETMOD",
      "If non-empty, this parameter specifies which network module should be used for communication.",
      { MPIR_PARAM_TYPE_STRING, -1, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_INTERFACE_HOSTNAME,
      "INTERFACE_HOSTNAME",
      "If non-NULL, this parameter specifies the IP address that other processes should use when connecting to this process. This parameter is mutually exclusive with the MPICH_NETWORK_IFACE parameter and it is an error to set them both.",
      { MPIR_PARAM_TYPE_STRING, -1, 0.0, NULL, {0,0} } },
    { MPIR_PARAM_ID_NETWORK_IFACE,
      "NETWORK_IFACE",
      "If non-NULL, this parameter specifies which pseudo-ethernet interface the tcp netmod should use (e.g., \"eth1\", \"ib0\").  Note, this is a Linux-specific parameter. This parameter is mutually exclusive with the MPICH_INTERFACE_HOSTNAME parameter and it is an error to set them both.",
      { MPIR_PARAM_TYPE_STRING, -1, 0.0, NULL, {0,0} } },
    { MPIR_PARAM_ID_HOST_LOOKUP_RETRIES,
      "HOST_LOOKUP_RETRIES",
      "This parameter controls the number of times to retry the  gethostbyname() function before giving up.",
      { MPIR_PARAM_TYPE_INT, 10, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_DEBUG_HOLD,
      "DEBUG_HOLD",
      "If true, causes processes to wait in MPI_Init and MPI_Initthread for a debugger to be attached.  Once the debugger has attached, the variable 'hold' should be set to 0 in order to allow the process to continue (e.g., in gdb, \"set hold=0\").",
      { MPIR_PARAM_TYPE_BOOLEAN, 0, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_ENABLE_CKPOINT,
      "ENABLE_CKPOINT",
      "If true, enables checkpointing support and returns an error if checkpointing library cannot be initialized.",
      { MPIR_PARAM_TYPE_BOOLEAN, 0, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_ENABLE_COLL_FT_RET,
      "ENABLE_COLL_FT_RET",
      "Collectives called on a communicator with a failed process should not hang, however the result of the operation may be invalid even though the function returns MPI_SUCCESS.  This option enables an experimental feature that will return an error if the result of the collective is invalid.",
      { MPIR_PARAM_TYPE_BOOLEAN, 0, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_ABORT_ON_LEAKED_HANDLES,
      "ABORT_ON_LEAKED_HANDLES",
      "If true, MPI will call MPI_Abort at MPI_Finalize if any MPI object handles have been leaked.  For example, if MPI_Comm_dup is called without calling a corresponding MPI_Comm_free.  For uninteresting reasons, enabling this option may prevent all known object leaks from being reported.  MPICH2 must have been configure with \"--enable-g=handlealloc\" or better in order for this functionality to work.",
      { MPIR_PARAM_TYPE_BOOLEAN, 0, 0.0, "", {0,0} } },
    { MPIR_PARAM_ID_PORT_RANGE,
      "PORT_RANGE",
      "\"The MPICH_PORT_RANGE environment variable allows you to specify the range of TCP ports to be used by the process manager and the MPICH2 library. The format of this variable is <low>:<high>.\"",
      { MPIR_PARAM_TYPE_RANGE, -1, 0.0, "", {0,0} } },
};

/* actual storage for parameters */
int MPIR_PARAM_ALLTOALL_SHORT_MSG_SIZE = 256;
int MPIR_PARAM_ALLTOALL_MEDIUM_MSG_SIZE = 32768;
int MPIR_PARAM_ALLTOALL_THROTTLE = 4;
int MPIR_PARAM_REDSCAT_COMMUTATIVE_LONG_MSG_SIZE = 524288;
int MPIR_PARAM_BCAST_MIN_PROCS = 8;
int MPIR_PARAM_BCAST_SHORT_MSG_SIZE = 12288;
int MPIR_PARAM_BCAST_LONG_MSG_SIZE = 524288;
int MPIR_PARAM_ALLGATHER_SHORT_MSG_SIZE = 81920;
int MPIR_PARAM_ALLGATHER_LONG_MSG_SIZE = 524288;
int MPIR_PARAM_REDUCE_SHORT_MSG_SIZE = 2048;
int MPIR_PARAM_ALLREDUCE_SHORT_MSG_SIZE = 2048;
int MPIR_PARAM_GATHER_VSMALL_MSG_SIZE = 1024;
int MPIR_PARAM_GATHER_INTER_SHORT_MSG_SIZE = 2048;
int MPIR_PARAM_GATHERV_INTER_SSEND_MIN_PROCS = 32;
int MPIR_PARAM_SCATTER_INTER_SHORT_MSG_SIZE = 2048;
int MPIR_PARAM_ALLGATHERV_PIPELINE_MSG_SIZE = 32768;
int MPIR_PARAM_COMM_SPLIT_USE_QSORT = 1;
int MPIR_PARAM_NOLOCAL = 0;
int MPIR_PARAM_ODD_EVEN_CLIQUES = 0;
int MPIR_PARAM_MEMDUMP = 1;
int MPIR_PARAM_PROCTABLE_SIZE = 64;
int MPIR_PARAM_PROCTABLE_PRINT = 1;
int MPIR_PARAM_ERROR_CHECKING = 1;
int MPIR_PARAM_PRINT_ERROR_STACK = 1;
int MPIR_PARAM_CHOP_ERROR_STACK = 0;
int MPIR_PARAM_NEM_LMT_DMA_THRESHOLD = 2097152;
const char * MPIR_PARAM_NEMESIS_NETMOD = "";
const char * MPIR_PARAM_INTERFACE_HOSTNAME = NULL;
const char * MPIR_PARAM_NETWORK_IFACE = NULL;
int MPIR_PARAM_HOST_LOOKUP_RETRIES = 10;
int MPIR_PARAM_DEBUG_HOLD = 0;
int MPIR_PARAM_ENABLE_CKPOINT = 0;
int MPIR_PARAM_ENABLE_COLL_FT_RET = 0;
int MPIR_PARAM_ABORT_ON_LEAKED_HANDLES = 0;
MPIR_Param_param_range_val_t MPIR_PARAM_PORT_RANGE = {0,0};

#undef FUNCNAME
#define FUNCNAME MPIR_Param_init_params
#undef FCNAME
#define FCNAME MPIU_QUOTE(FUNCNAME)
int MPIR_Param_init_params(void)
{
    int mpi_errno = MPI_SUCCESS;
    int rc;

    rc = MPL_env2int("MPICH_ALLTOALL_SHORT_MSG_SIZE", &(MPIR_PARAM_ALLTOALL_SHORT_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_ALLTOALL_SHORT_MSG_SIZE");
    rc = MPL_env2int("MPIR_PARAM_ALLTOALL_SHORT_MSG_SIZE", &(MPIR_PARAM_ALLTOALL_SHORT_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_ALLTOALL_SHORT_MSG_SIZE");

    rc = MPL_env2int("MPICH_ALLTOALL_MEDIUM_MSG_SIZE", &(MPIR_PARAM_ALLTOALL_MEDIUM_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_ALLTOALL_MEDIUM_MSG_SIZE");
    rc = MPL_env2int("MPIR_PARAM_ALLTOALL_MEDIUM_MSG_SIZE", &(MPIR_PARAM_ALLTOALL_MEDIUM_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_ALLTOALL_MEDIUM_MSG_SIZE");

    rc = MPL_env2int("MPICH_ALLTOALL_THROTTLE", &(MPIR_PARAM_ALLTOALL_THROTTLE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_ALLTOALL_THROTTLE");
    rc = MPL_env2int("MPIR_PARAM_ALLTOALL_THROTTLE", &(MPIR_PARAM_ALLTOALL_THROTTLE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_ALLTOALL_THROTTLE");

    rc = MPL_env2int("MPICH_REDSCAT_COMMUTATIVE_LONG_MSG_SIZE", &(MPIR_PARAM_REDSCAT_COMMUTATIVE_LONG_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_REDSCAT_COMMUTATIVE_LONG_MSG_SIZE");
    rc = MPL_env2int("MPIR_PARAM_REDSCAT_COMMUTATIVE_LONG_MSG_SIZE", &(MPIR_PARAM_REDSCAT_COMMUTATIVE_LONG_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_REDSCAT_COMMUTATIVE_LONG_MSG_SIZE");

    rc = MPL_env2int("MPICH_BCAST_MIN_PROCS", &(MPIR_PARAM_BCAST_MIN_PROCS));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_BCAST_MIN_PROCS");
    rc = MPL_env2int("MPIR_PARAM_BCAST_MIN_PROCS", &(MPIR_PARAM_BCAST_MIN_PROCS));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_BCAST_MIN_PROCS");

    rc = MPL_env2int("MPICH_BCAST_SHORT_MSG_SIZE", &(MPIR_PARAM_BCAST_SHORT_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_BCAST_SHORT_MSG_SIZE");
    rc = MPL_env2int("MPIR_PARAM_BCAST_SHORT_MSG_SIZE", &(MPIR_PARAM_BCAST_SHORT_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_BCAST_SHORT_MSG_SIZE");

    rc = MPL_env2int("MPICH_BCAST_LONG_MSG_SIZE", &(MPIR_PARAM_BCAST_LONG_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_BCAST_LONG_MSG_SIZE");
    rc = MPL_env2int("MPIR_PARAM_BCAST_LONG_MSG_SIZE", &(MPIR_PARAM_BCAST_LONG_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_BCAST_LONG_MSG_SIZE");

    rc = MPL_env2int("MPICH_ALLGATHER_SHORT_MSG_SIZE", &(MPIR_PARAM_ALLGATHER_SHORT_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_ALLGATHER_SHORT_MSG_SIZE");
    rc = MPL_env2int("MPIR_PARAM_ALLGATHER_SHORT_MSG_SIZE", &(MPIR_PARAM_ALLGATHER_SHORT_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_ALLGATHER_SHORT_MSG_SIZE");

    rc = MPL_env2int("MPICH_ALLGATHER_LONG_MSG_SIZE", &(MPIR_PARAM_ALLGATHER_LONG_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_ALLGATHER_LONG_MSG_SIZE");
    rc = MPL_env2int("MPIR_PARAM_ALLGATHER_LONG_MSG_SIZE", &(MPIR_PARAM_ALLGATHER_LONG_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_ALLGATHER_LONG_MSG_SIZE");

    rc = MPL_env2int("MPICH_REDUCE_SHORT_MSG_SIZE", &(MPIR_PARAM_REDUCE_SHORT_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_REDUCE_SHORT_MSG_SIZE");
    rc = MPL_env2int("MPIR_PARAM_REDUCE_SHORT_MSG_SIZE", &(MPIR_PARAM_REDUCE_SHORT_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_REDUCE_SHORT_MSG_SIZE");

    rc = MPL_env2int("MPICH_ALLREDUCE_SHORT_MSG_SIZE", &(MPIR_PARAM_ALLREDUCE_SHORT_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_ALLREDUCE_SHORT_MSG_SIZE");
    rc = MPL_env2int("MPIR_PARAM_ALLREDUCE_SHORT_MSG_SIZE", &(MPIR_PARAM_ALLREDUCE_SHORT_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_ALLREDUCE_SHORT_MSG_SIZE");

    rc = MPL_env2int("MPICH_GATHER_VSMALL_MSG_SIZE", &(MPIR_PARAM_GATHER_VSMALL_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_GATHER_VSMALL_MSG_SIZE");
    rc = MPL_env2int("MPIR_PARAM_GATHER_VSMALL_MSG_SIZE", &(MPIR_PARAM_GATHER_VSMALL_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_GATHER_VSMALL_MSG_SIZE");

    rc = MPL_env2int("MPICH_GATHER_INTER_SHORT_MSG_SIZE", &(MPIR_PARAM_GATHER_INTER_SHORT_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_GATHER_INTER_SHORT_MSG_SIZE");
    rc = MPL_env2int("MPIR_PARAM_GATHER_INTER_SHORT_MSG_SIZE", &(MPIR_PARAM_GATHER_INTER_SHORT_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_GATHER_INTER_SHORT_MSG_SIZE");

    rc = MPL_env2int("MPICH2_GATHERV_MIN_PROCS", &(MPIR_PARAM_GATHERV_INTER_SSEND_MIN_PROCS));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH2_GATHERV_MIN_PROCS");
    rc = MPL_env2int("MPICH_GATHERV_INTER_SSEND_MIN_PROCS", &(MPIR_PARAM_GATHERV_INTER_SSEND_MIN_PROCS));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_GATHERV_INTER_SSEND_MIN_PROCS");
    rc = MPL_env2int("MPIR_PARAM_GATHERV_INTER_SSEND_MIN_PROCS", &(MPIR_PARAM_GATHERV_INTER_SSEND_MIN_PROCS));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_GATHERV_INTER_SSEND_MIN_PROCS");

    rc = MPL_env2int("MPICH_SCATTER_INTER_SHORT_MSG_SIZE", &(MPIR_PARAM_SCATTER_INTER_SHORT_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_SCATTER_INTER_SHORT_MSG_SIZE");
    rc = MPL_env2int("MPIR_PARAM_SCATTER_INTER_SHORT_MSG_SIZE", &(MPIR_PARAM_SCATTER_INTER_SHORT_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_SCATTER_INTER_SHORT_MSG_SIZE");

    rc = MPL_env2int("MPICH_ALLGATHERV_PIPELINE_MSG_SIZE", &(MPIR_PARAM_ALLGATHERV_PIPELINE_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_ALLGATHERV_PIPELINE_MSG_SIZE");
    rc = MPL_env2int("MPIR_PARAM_ALLGATHERV_PIPELINE_MSG_SIZE", &(MPIR_PARAM_ALLGATHERV_PIPELINE_MSG_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_ALLGATHERV_PIPELINE_MSG_SIZE");

    rc = MPL_env2bool("MPICH_COMM_SPLIT_USE_QSORT", &(MPIR_PARAM_COMM_SPLIT_USE_QSORT));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_COMM_SPLIT_USE_QSORT");
    rc = MPL_env2bool("MPIR_PARAM_COMM_SPLIT_USE_QSORT", &(MPIR_PARAM_COMM_SPLIT_USE_QSORT));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_COMM_SPLIT_USE_QSORT");

    rc = MPL_env2bool("MPICH_NO_LOCAL", &(MPIR_PARAM_NOLOCAL));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_NO_LOCAL");
    rc = MPL_env2bool("MPIR_PARAM_NO_LOCAL", &(MPIR_PARAM_NOLOCAL));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_NO_LOCAL");
    rc = MPL_env2bool("MPICH_NOLOCAL", &(MPIR_PARAM_NOLOCAL));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_NOLOCAL");
    rc = MPL_env2bool("MPIR_PARAM_NOLOCAL", &(MPIR_PARAM_NOLOCAL));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_NOLOCAL");

    rc = MPL_env2bool("MPICH_EVEN_ODD_CLIQUES", &(MPIR_PARAM_ODD_EVEN_CLIQUES));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_EVEN_ODD_CLIQUES");
    rc = MPL_env2bool("MPIR_PARAM_EVEN_ODD_CLIQUES", &(MPIR_PARAM_ODD_EVEN_CLIQUES));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_EVEN_ODD_CLIQUES");
    rc = MPL_env2bool("MPICH_ODD_EVEN_CLIQUES", &(MPIR_PARAM_ODD_EVEN_CLIQUES));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_ODD_EVEN_CLIQUES");
    rc = MPL_env2bool("MPIR_PARAM_ODD_EVEN_CLIQUES", &(MPIR_PARAM_ODD_EVEN_CLIQUES));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_ODD_EVEN_CLIQUES");

    rc = MPL_env2bool("MPICH_MEMDUMP", &(MPIR_PARAM_MEMDUMP));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_MEMDUMP");
    rc = MPL_env2bool("MPIR_PARAM_MEMDUMP", &(MPIR_PARAM_MEMDUMP));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_MEMDUMP");

    rc = MPL_env2int("MPICH_PROCTABLE_SIZE", &(MPIR_PARAM_PROCTABLE_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_PROCTABLE_SIZE");
    rc = MPL_env2int("MPIR_PARAM_PROCTABLE_SIZE", &(MPIR_PARAM_PROCTABLE_SIZE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_PROCTABLE_SIZE");

    rc = MPL_env2bool("MPICH_PROCTABLE_PRINT", &(MPIR_PARAM_PROCTABLE_PRINT));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_PROCTABLE_PRINT");
    rc = MPL_env2bool("MPIR_PARAM_PROCTABLE_PRINT", &(MPIR_PARAM_PROCTABLE_PRINT));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_PROCTABLE_PRINT");

    rc = MPL_env2bool("MPICH_ERROR_CHECKING", &(MPIR_PARAM_ERROR_CHECKING));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_ERROR_CHECKING");
    rc = MPL_env2bool("MPIR_PARAM_ERROR_CHECKING", &(MPIR_PARAM_ERROR_CHECKING));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_ERROR_CHECKING");

    rc = MPL_env2bool("MPICH_PRINT_ERROR_STACK", &(MPIR_PARAM_PRINT_ERROR_STACK));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_PRINT_ERROR_STACK");
    rc = MPL_env2bool("MPIR_PARAM_PRINT_ERROR_STACK", &(MPIR_PARAM_PRINT_ERROR_STACK));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_PRINT_ERROR_STACK");

    rc = MPL_env2int("MPICH_CHOP_ERROR_STACK", &(MPIR_PARAM_CHOP_ERROR_STACK));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_CHOP_ERROR_STACK");
    rc = MPL_env2int("MPIR_PARAM_CHOP_ERROR_STACK", &(MPIR_PARAM_CHOP_ERROR_STACK));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_CHOP_ERROR_STACK");

    rc = MPL_env2int("MPICH_NEM_LMT_DMA_THRESHOLD", &(MPIR_PARAM_NEM_LMT_DMA_THRESHOLD));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_NEM_LMT_DMA_THRESHOLD");
    rc = MPL_env2int("MPIR_PARAM_NEM_LMT_DMA_THRESHOLD", &(MPIR_PARAM_NEM_LMT_DMA_THRESHOLD));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_NEM_LMT_DMA_THRESHOLD");

    rc = MPL_env2str("MPICH_NEMESIS_NETMOD", &(MPIR_PARAM_NEMESIS_NETMOD));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_NEMESIS_NETMOD");
    rc = MPL_env2str("MPIR_PARAM_NEMESIS_NETMOD", &(MPIR_PARAM_NEMESIS_NETMOD));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_NEMESIS_NETMOD");

    rc = MPL_env2str("MPICH_INTERFACE_HOSTNAME", &(MPIR_PARAM_INTERFACE_HOSTNAME));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_INTERFACE_HOSTNAME");
    rc = MPL_env2str("MPIR_PARAM_INTERFACE_HOSTNAME", &(MPIR_PARAM_INTERFACE_HOSTNAME));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_INTERFACE_HOSTNAME");

    rc = MPL_env2str("MPICH_NETWORK_IFACE", &(MPIR_PARAM_NETWORK_IFACE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_NETWORK_IFACE");
    rc = MPL_env2str("MPIR_PARAM_NETWORK_IFACE", &(MPIR_PARAM_NETWORK_IFACE));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_NETWORK_IFACE");

    rc = MPL_env2int("MPICH_HOST_LOOKUP_RETRIES", &(MPIR_PARAM_HOST_LOOKUP_RETRIES));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_HOST_LOOKUP_RETRIES");
    rc = MPL_env2int("MPIR_PARAM_HOST_LOOKUP_RETRIES", &(MPIR_PARAM_HOST_LOOKUP_RETRIES));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_HOST_LOOKUP_RETRIES");

    rc = MPL_env2bool("MPICH_DEBUG_HOLD", &(MPIR_PARAM_DEBUG_HOLD));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_DEBUG_HOLD");
    rc = MPL_env2bool("MPIR_PARAM_DEBUG_HOLD", &(MPIR_PARAM_DEBUG_HOLD));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_DEBUG_HOLD");

    rc = MPL_env2bool("MPICH_ENABLE_CKPOINT", &(MPIR_PARAM_ENABLE_CKPOINT));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_ENABLE_CKPOINT");
    rc = MPL_env2bool("MPIR_PARAM_ENABLE_CKPOINT", &(MPIR_PARAM_ENABLE_CKPOINT));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_ENABLE_CKPOINT");

    rc = MPL_env2bool("MPICH_ENABLE_COLL_FT_RET", &(MPIR_PARAM_ENABLE_COLL_FT_RET));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_ENABLE_COLL_FT_RET");
    rc = MPL_env2bool("MPIR_PARAM_ENABLE_COLL_FT_RET", &(MPIR_PARAM_ENABLE_COLL_FT_RET));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_ENABLE_COLL_FT_RET");

    rc = MPL_env2bool("MPICH_ABORT_ON_LEAKED_HANDLES", &(MPIR_PARAM_ABORT_ON_LEAKED_HANDLES));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_ABORT_ON_LEAKED_HANDLES");
    rc = MPL_env2bool("MPIR_PARAM_ABORT_ON_LEAKED_HANDLES", &(MPIR_PARAM_ABORT_ON_LEAKED_HANDLES));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_ABORT_ON_LEAKED_HANDLES");

    rc = MPL_env2range("MPICH_PORTRANGE", &(MPIR_PARAM_PORT_RANGE.low), &(MPIR_PARAM_PORT_RANGE.high));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_PORTRANGE");
    rc = MPL_env2range("MPIR_PARAM_PORTRANGE", &(MPIR_PARAM_PORT_RANGE.low), &(MPIR_PARAM_PORT_RANGE.high));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_PORTRANGE");
    rc = MPL_env2range("MPICH_PORT_RANGE", &(MPIR_PARAM_PORT_RANGE.low), &(MPIR_PARAM_PORT_RANGE.high));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPICH_PORT_RANGE");
    rc = MPL_env2range("MPIR_PARAM_PORT_RANGE", &(MPIR_PARAM_PORT_RANGE.low), &(MPIR_PARAM_PORT_RANGE.high));
    MPIU_ERR_CHKANDJUMP1((-1 == rc),mpi_errno,MPI_ERR_OTHER,"**envvarparse","**envvarparse %s","MPIR_PARAM_PORT_RANGE");

fn_fail:
    return mpi_errno;
}

