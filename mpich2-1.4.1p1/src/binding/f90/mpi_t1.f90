!  (C) 2008 by Argonne National Laboratory.
!      See COPYRIGHT in top-level directory.
        MODULE MPI_t1_s
        IMPLICIT NONE
        PRIVATE
        PUBLIC :: MPI_SSEND_INIT
        INTERFACE MPI_SSEND_INIT
           MODULE PROCEDURE MPI_SSEND_INIT_T
        END INTERFACE MPI_SSEND_INIT

        PUBLIC :: MPI_TYPE_CREATE_KEYVAL
        INTERFACE MPI_TYPE_CREATE_KEYVAL
           MODULE PROCEDURE MPI_TYPE_CREATE_KEYVAL_T
        END INTERFACE MPI_TYPE_CREATE_KEYVAL

        PUBLIC :: MPI_IBSEND
        INTERFACE MPI_IBSEND
           MODULE PROCEDURE MPI_IBSEND_T
        END INTERFACE MPI_IBSEND

        PUBLIC :: MPI_FINALIZE
        INTERFACE MPI_FINALIZE
           MODULE PROCEDURE MPI_FINALIZE_T
        END INTERFACE MPI_FINALIZE

        PUBLIC :: MPI_PUT
        INTERFACE MPI_PUT
           MODULE PROCEDURE MPI_PUT_T
        END INTERFACE MPI_PUT

        PUBLIC :: MPI_ALLREDUCE
        INTERFACE MPI_ALLREDUCE
           MODULE PROCEDURE MPI_ALLREDUCE_T
        END INTERFACE MPI_ALLREDUCE

        PUBLIC :: MPI_WIN_CREATE
        INTERFACE MPI_WIN_CREATE
           MODULE PROCEDURE MPI_WIN_CREATE_T
        END INTERFACE MPI_WIN_CREATE

        PUBLIC :: MPI_RECV
        INTERFACE MPI_RECV
           MODULE PROCEDURE MPI_RECV_T
        END INTERFACE MPI_RECV

        PUBLIC :: MPI_SENDRECV
        INTERFACE MPI_SENDRECV
           MODULE PROCEDURE MPI_SENDRECV_T
        END INTERFACE MPI_SENDRECV

        PUBLIC :: MPI_ALLTOALL
        INTERFACE MPI_ALLTOALL
           MODULE PROCEDURE MPI_ALLTOALL_T
        END INTERFACE MPI_ALLTOALL

        PUBLIC :: MPI_UNPACK
        INTERFACE MPI_UNPACK
           MODULE PROCEDURE MPI_UNPACK_T
        END INTERFACE MPI_UNPACK

        PUBLIC :: MPI_ISEND
        INTERFACE MPI_ISEND
           MODULE PROCEDURE MPI_ISEND_T
        END INTERFACE MPI_ISEND

        PUBLIC :: MPI_GET
        INTERFACE MPI_GET
           MODULE PROCEDURE MPI_GET_T
        END INTERFACE MPI_GET

        PUBLIC :: MPI_REDUCE_LOCAL
        INTERFACE MPI_REDUCE_LOCAL
           MODULE PROCEDURE MPI_REDUCE_LOCAL_T
        END INTERFACE MPI_REDUCE_LOCAL

        PUBLIC :: MPI_GREQUEST_START
        INTERFACE MPI_GREQUEST_START
           MODULE PROCEDURE MPI_GREQUEST_START_T
        END INTERFACE MPI_GREQUEST_START

        PUBLIC :: MPI_TYPE_GET_ATTR
        INTERFACE MPI_TYPE_GET_ATTR
           MODULE PROCEDURE MPI_TYPE_GET_ATTR_T
        END INTERFACE MPI_TYPE_GET_ATTR

        PUBLIC :: MPI_ALLOC_MEM
        INTERFACE MPI_ALLOC_MEM
           MODULE PROCEDURE MPI_ALLOC_MEM_T
        END INTERFACE MPI_ALLOC_MEM

        PUBLIC :: MPI_REDUCE_SCATTER
        INTERFACE MPI_REDUCE_SCATTER
           MODULE PROCEDURE MPI_REDUCE_SCATTER_T
        END INTERFACE MPI_REDUCE_SCATTER

        PUBLIC :: MPI_SEND
        INTERFACE MPI_SEND
           MODULE PROCEDURE MPI_SEND_T
        END INTERFACE MPI_SEND

        PUBLIC :: MPI_SSEND
        INTERFACE MPI_SSEND
           MODULE PROCEDURE MPI_SSEND_T
        END INTERFACE MPI_SSEND

        PUBLIC :: MPI_UNPACK_EXTERNAL
        INTERFACE MPI_UNPACK_EXTERNAL
           MODULE PROCEDURE MPI_UNPACK_EXTERNAL_T
        END INTERFACE MPI_UNPACK_EXTERNAL

        PUBLIC :: MPI_ALLGATHER
        INTERFACE MPI_ALLGATHER
           MODULE PROCEDURE MPI_ALLGATHER_T
        END INTERFACE MPI_ALLGATHER

        PUBLIC :: MPI_GET_ADDRESS
        INTERFACE MPI_GET_ADDRESS
           MODULE PROCEDURE MPI_GET_ADDRESS_T
        END INTERFACE MPI_GET_ADDRESS

        PUBLIC :: MPI_EXSCAN
        INTERFACE MPI_EXSCAN
           MODULE PROCEDURE MPI_EXSCAN_T
        END INTERFACE MPI_EXSCAN

        PUBLIC :: MPI_PACK_EXTERNAL
        INTERFACE MPI_PACK_EXTERNAL
           MODULE PROCEDURE MPI_PACK_EXTERNAL_T
        END INTERFACE MPI_PACK_EXTERNAL

        PUBLIC :: MPI_IRECV
        INTERFACE MPI_IRECV
           MODULE PROCEDURE MPI_IRECV_T
        END INTERFACE MPI_IRECV

        PUBLIC :: MPI_WIN_CREATE_KEYVAL
        INTERFACE MPI_WIN_CREATE_KEYVAL
           MODULE PROCEDURE MPI_WIN_CREATE_KEYVAL_T
        END INTERFACE MPI_WIN_CREATE_KEYVAL

        PUBLIC :: MPI_SCATTERV
        INTERFACE MPI_SCATTERV
           MODULE PROCEDURE MPI_SCATTERV_T
        END INTERFACE MPI_SCATTERV

        PUBLIC :: MPI_SCAN
        INTERFACE MPI_SCAN
           MODULE PROCEDURE MPI_SCAN_T
        END INTERFACE MPI_SCAN

        PUBLIC :: MPI_RSEND
        INTERFACE MPI_RSEND
           MODULE PROCEDURE MPI_RSEND_T
        END INTERFACE MPI_RSEND

        PUBLIC :: MPI_PACK
        INTERFACE MPI_PACK
           MODULE PROCEDURE MPI_PACK_T
        END INTERFACE MPI_PACK

        PUBLIC :: MPI_ALLGATHERV
        INTERFACE MPI_ALLGATHERV
           MODULE PROCEDURE MPI_ALLGATHERV_T
        END INTERFACE MPI_ALLGATHERV

        PUBLIC :: MPI_BSEND_INIT
        INTERFACE MPI_BSEND_INIT
           MODULE PROCEDURE MPI_BSEND_INIT_T
        END INTERFACE MPI_BSEND_INIT

        PUBLIC :: MPI_RSEND_INIT
        INTERFACE MPI_RSEND_INIT
           MODULE PROCEDURE MPI_RSEND_INIT_T
        END INTERFACE MPI_RSEND_INIT

        PUBLIC :: MPI_GATHERV
        INTERFACE MPI_GATHERV
           MODULE PROCEDURE MPI_GATHERV_T
        END INTERFACE MPI_GATHERV

        PUBLIC :: MPI_REDUCE_SCATTER_BLOCK
        INTERFACE MPI_REDUCE_SCATTER_BLOCK
           MODULE PROCEDURE MPI_REDUCE_SCATTER_BLOCK_T
        END INTERFACE MPI_REDUCE_SCATTER_BLOCK

        PUBLIC :: MPI_ADDRESS
        INTERFACE MPI_ADDRESS
           MODULE PROCEDURE MPI_ADDRESS_T
        END INTERFACE MPI_ADDRESS

        PUBLIC :: MPI_WIN_GET_ATTR
        INTERFACE MPI_WIN_GET_ATTR
           MODULE PROCEDURE MPI_WIN_GET_ATTR_T
        END INTERFACE MPI_WIN_GET_ATTR

        PUBLIC :: MPI_SEND_INIT
        INTERFACE MPI_SEND_INIT
           MODULE PROCEDURE MPI_SEND_INIT_T
        END INTERFACE MPI_SEND_INIT

        PUBLIC :: MPI_ALLTOALLW
        INTERFACE MPI_ALLTOALLW
           MODULE PROCEDURE MPI_ALLTOALLW_T
        END INTERFACE MPI_ALLTOALLW

        PUBLIC :: MPI_ATTR_GET
        INTERFACE MPI_ATTR_GET
           MODULE PROCEDURE MPI_ATTR_GET_T
        END INTERFACE MPI_ATTR_GET

        PUBLIC :: MPI_COMM_SET_ATTR
        INTERFACE MPI_COMM_SET_ATTR
           MODULE PROCEDURE MPI_COMM_SET_ATTR_T
        END INTERFACE MPI_COMM_SET_ATTR

        PUBLIC :: MPI_SENDRECV_REPLACE
        INTERFACE MPI_SENDRECV_REPLACE
           MODULE PROCEDURE MPI_SENDRECV_REPLACE_T
        END INTERFACE MPI_SENDRECV_REPLACE

        PUBLIC :: MPI_TYPE_SET_ATTR
        INTERFACE MPI_TYPE_SET_ATTR
           MODULE PROCEDURE MPI_TYPE_SET_ATTR_T
        END INTERFACE MPI_TYPE_SET_ATTR

        PUBLIC :: MPI_ISSEND
        INTERFACE MPI_ISSEND
           MODULE PROCEDURE MPI_ISSEND_T
        END INTERFACE MPI_ISSEND

        PUBLIC :: MPI_BCAST
        INTERFACE MPI_BCAST
           MODULE PROCEDURE MPI_BCAST_T
        END INTERFACE MPI_BCAST

        PUBLIC :: MPI_COMM_GET_ATTR
        INTERFACE MPI_COMM_GET_ATTR
           MODULE PROCEDURE MPI_COMM_GET_ATTR_T
        END INTERFACE MPI_COMM_GET_ATTR

        PUBLIC :: MPI_ALLTOALLV
        INTERFACE MPI_ALLTOALLV
           MODULE PROCEDURE MPI_ALLTOALLV_T
        END INTERFACE MPI_ALLTOALLV

        PUBLIC :: MPI_ACCUMULATE
        INTERFACE MPI_ACCUMULATE
           MODULE PROCEDURE MPI_ACCUMULATE_T
        END INTERFACE MPI_ACCUMULATE

        PUBLIC :: MPI_IRSEND
        INTERFACE MPI_IRSEND
           MODULE PROCEDURE MPI_IRSEND_T
        END INTERFACE MPI_IRSEND

        PUBLIC :: MPI_COMM_CREATE_KEYVAL
        INTERFACE MPI_COMM_CREATE_KEYVAL
           MODULE PROCEDURE MPI_COMM_CREATE_KEYVAL_T
        END INTERFACE MPI_COMM_CREATE_KEYVAL

        PUBLIC :: MPI_SCATTER
        INTERFACE MPI_SCATTER
           MODULE PROCEDURE MPI_SCATTER_T
        END INTERFACE MPI_SCATTER

        PUBLIC :: MPI_RECV_INIT
        INTERFACE MPI_RECV_INIT
           MODULE PROCEDURE MPI_RECV_INIT_T
        END INTERFACE MPI_RECV_INIT

        PUBLIC :: MPI_GATHER
        INTERFACE MPI_GATHER
           MODULE PROCEDURE MPI_GATHER_T
        END INTERFACE MPI_GATHER

        PUBLIC :: MPI_KEYVAL_CREATE
        INTERFACE MPI_KEYVAL_CREATE
           MODULE PROCEDURE MPI_KEYVAL_CREATE_T
        END INTERFACE MPI_KEYVAL_CREATE

        PUBLIC :: MPI_REDUCE
        INTERFACE MPI_REDUCE
           MODULE PROCEDURE MPI_REDUCE_T
        END INTERFACE MPI_REDUCE

        PUBLIC :: MPI_BSEND
        INTERFACE MPI_BSEND
           MODULE PROCEDURE MPI_BSEND_T
        END INTERFACE MPI_BSEND

        PUBLIC :: MPI_FREE_MEM
        INTERFACE MPI_FREE_MEM
           MODULE PROCEDURE MPI_FREE_MEM_T
        END INTERFACE MPI_FREE_MEM

        PUBLIC :: MPI_BUFFER_DETACH
        INTERFACE MPI_BUFFER_DETACH
           MODULE PROCEDURE MPI_BUFFER_DETACH_T
        END INTERFACE MPI_BUFFER_DETACH

        PUBLIC :: MPI_BUFFER_ATTACH
        INTERFACE MPI_BUFFER_ATTACH
           MODULE PROCEDURE MPI_BUFFER_ATTACH_T
        END INTERFACE MPI_BUFFER_ATTACH

        PUBLIC :: MPI_ATTR_PUT
        INTERFACE MPI_ATTR_PUT
           MODULE PROCEDURE MPI_ATTR_PUT_T
        END INTERFACE MPI_ATTR_PUT

        PUBLIC :: MPI_WIN_SET_ATTR
        INTERFACE MPI_WIN_SET_ATTR
           MODULE PROCEDURE MPI_WIN_SET_ATTR_T
        END INTERFACE MPI_WIN_SET_ATTR

        CONTAINS

        SUBROUTINE MPI_SSEND_INIT_T(v0,v1,v2,v3,v4,v5,v6,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER v6
        INTEGER ierror
        EXTERNAL MPI_SSEND_INIT
        CALL MPI_SSEND_INIT(v0,v1,v2,v3,v4,v5,v6,ierror)
        END SUBROUTINE MPI_SSEND_INIT_T

        SUBROUTINE MPI_TYPE_CREATE_KEYVAL_T(v0,v1,v2,v3,ierror)
        EXTERNAL v0
        EXTERNAL v1
        INTEGER v2
        <type> v3<dims>
        INTEGER ierror
        EXTERNAL MPI_TYPE_CREATE_KEYVAL
        CALL MPI_TYPE_CREATE_KEYVAL(v0,v1,v2,v3,ierror)
        END SUBROUTINE MPI_TYPE_CREATE_KEYVAL_T

        SUBROUTINE MPI_IBSEND_T(v0,v1,v2,v3,v4,v5,v6,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER v6
        INTEGER ierror
        EXTERNAL MPI_IBSEND
        CALL MPI_IBSEND(v0,v1,v2,v3,v4,v5,v6,ierror)
        END SUBROUTINE MPI_IBSEND_T

        SUBROUTINE MPI_FINALIZE_T(v0,ierror)
        <type> v0<dims>
        INTEGER ierror
        EXTERNAL MPI_FINALIZE
        CALL MPI_FINALIZE(v0,ierror)
        END SUBROUTINE MPI_FINALIZE_T

        SUBROUTINE MPI_PUT_T(v0,v1,v2,v3,v4,v5,v6,v7,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER(KIND=MPI_ADDRESS_KIND) v4
        INTEGER v5
        INTEGER v6
        INTEGER v7
        INTEGER ierror
        EXTERNAL MPI_PUT
        CALL MPI_PUT(v0,v1,v2,v3,v4,v5,v6,v7,ierror)
        END SUBROUTINE MPI_PUT_T

        SUBROUTINE MPI_ALLREDUCE_T(v0,v1,v2,v3,v4,v5,ierror)
        <type> v0<dims>
        <type1> v1<dims1>
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER ierror
        EXTERNAL MPI_ALLREDUCE
        CALL MPI_ALLREDUCE(v0,v1,v2,v3,v4,v5,ierror)
        END SUBROUTINE MPI_ALLREDUCE_T

        SUBROUTINE MPI_WIN_CREATE_T(v0,v1,v2,v3,v4,v5,ierror)
        <type> v0<dims>
        INTEGER(KIND=MPI_ADDRESS_KIND) v1
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER ierror
        EXTERNAL MPI_WIN_CREATE
        CALL MPI_WIN_CREATE(v0,v1,v2,v3,v4,v5,ierror)
        END SUBROUTINE MPI_WIN_CREATE_T

        SUBROUTINE MPI_RECV_T(v0,v1,v2,v3,v4,v5,v6,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER v6(MPI_STATUS_SIZE)
        INTEGER ierror
        EXTERNAL MPI_RECV
        CALL MPI_RECV(v0,v1,v2,v3,v4,v5,v6,ierror)
        END SUBROUTINE MPI_RECV_T

        SUBROUTINE MPI_SENDRECV_T(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER v4
        <type1> v5<dims1>
        INTEGER v6
        INTEGER v7
        INTEGER v8
        INTEGER v9
        INTEGER v10
        INTEGER v11(MPI_STATUS_SIZE)
        INTEGER ierror
        EXTERNAL MPI_SENDRECV
        CALL MPI_SENDRECV(v0,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,ierror)
        END SUBROUTINE MPI_SENDRECV_T

        SUBROUTINE MPI_ALLTOALL_T(v0,v1,v2,v3,v4,v5,v6,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        <type1> v3<dims1>
        INTEGER v4
        INTEGER v5
        INTEGER v6
        INTEGER ierror
        EXTERNAL MPI_ALLTOALL
        CALL MPI_ALLTOALL(v0,v1,v2,v3,v4,v5,v6,ierror)
        END SUBROUTINE MPI_ALLTOALL_T

        SUBROUTINE MPI_UNPACK_T(v0,v1,v2,v3,v4,v5,v6,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        <type1> v3<dims1>
        INTEGER v4
        INTEGER v5
        INTEGER v6
        INTEGER ierror
        EXTERNAL MPI_UNPACK
        CALL MPI_UNPACK(v0,v1,v2,v3,v4,v5,v6,ierror)
        END SUBROUTINE MPI_UNPACK_T

        SUBROUTINE MPI_ISEND_T(v0,v1,v2,v3,v4,v5,v6,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER v6
        INTEGER ierror
        EXTERNAL MPI_ISEND
        CALL MPI_ISEND(v0,v1,v2,v3,v4,v5,v6,ierror)
        END SUBROUTINE MPI_ISEND_T

        SUBROUTINE MPI_GET_T(v0,v1,v2,v3,v4,v5,v6,v7,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER(KIND=MPI_ADDRESS_KIND) v4
        INTEGER v5
        INTEGER v6
        INTEGER v7
        INTEGER ierror
        EXTERNAL MPI_GET
        CALL MPI_GET(v0,v1,v2,v3,v4,v5,v6,v7,ierror)
        END SUBROUTINE MPI_GET_T

        SUBROUTINE MPI_REDUCE_LOCAL_T(v0,v1,v2,v3,v4,ierror)
        <type> v0<dims>
        <type1> v1<dims1>
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER ierror
        EXTERNAL MPI_REDUCE_LOCAL
        CALL MPI_REDUCE_LOCAL(v0,v1,v2,v3,v4,ierror)
        END SUBROUTINE MPI_REDUCE_LOCAL_T

        SUBROUTINE MPI_GREQUEST_START_T(v0,v1,v2,v3,v4,ierror)
        EXTERNAL v0
        EXTERNAL v1
        EXTERNAL v2
        <type> v3<dims>
        INTEGER v4
        INTEGER ierror
        EXTERNAL MPI_GREQUEST_START
        CALL MPI_GREQUEST_START(v0,v1,v2,v3,v4,ierror)
        END SUBROUTINE MPI_GREQUEST_START_T

        SUBROUTINE MPI_TYPE_GET_ATTR_T(v0,v1,v2,v3,ierror)
        INTEGER v0
        INTEGER v1
        <type> v2<dims>
        LOGICAL v3
        INTEGER ierror
        EXTERNAL MPI_TYPE_GET_ATTR
        CALL MPI_TYPE_GET_ATTR(v0,v1,v2,v3,ierror)
        END SUBROUTINE MPI_TYPE_GET_ATTR_T

        SUBROUTINE MPI_ALLOC_MEM_T(v0,v1,v2,ierror)
        INTEGER(KIND=MPI_ADDRESS_KIND) v0
        INTEGER v1
        <type> v2<dims>
        INTEGER ierror
        EXTERNAL MPI_ALLOC_MEM
        CALL MPI_ALLOC_MEM(v0,v1,v2,ierror)
        END SUBROUTINE MPI_ALLOC_MEM_T

        SUBROUTINE MPI_REDUCE_SCATTER_T(v0,v1,v2,v3,v4,v5,ierror)
        <type> v0<dims>
        <type1> v1<dims1>
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER ierror
        EXTERNAL MPI_REDUCE_SCATTER
        CALL MPI_REDUCE_SCATTER(v0,v1,v2,v3,v4,v5,ierror)
        END SUBROUTINE MPI_REDUCE_SCATTER_T

        SUBROUTINE MPI_SEND_T(v0,v1,v2,v3,v4,v5,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER ierror
        EXTERNAL MPI_SEND
        CALL MPI_SEND(v0,v1,v2,v3,v4,v5,ierror)
        END SUBROUTINE MPI_SEND_T

        SUBROUTINE MPI_SSEND_T(v0,v1,v2,v3,v4,v5,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER ierror
        EXTERNAL MPI_SSEND
        CALL MPI_SSEND(v0,v1,v2,v3,v4,v5,ierror)
        END SUBROUTINE MPI_SSEND_T

        SUBROUTINE MPI_UNPACK_EXTERNAL_T(v0,v1,v2,v3,v4,v5,v6,ierror)
        CHARACTER (LEN=*) v0
        <type> v1<dims>
        INTEGER(KIND=MPI_ADDRESS_KIND) v2
        INTEGER(KIND=MPI_ADDRESS_KIND) v3
        <type1> v4<dims1>
        INTEGER v5
        INTEGER v6
        INTEGER ierror
        EXTERNAL MPI_UNPACK_EXTERNAL
        CALL MPI_UNPACK_EXTERNAL(v0,v1,v2,v3,v4,v5,v6,ierror)
        END SUBROUTINE MPI_UNPACK_EXTERNAL_T

        SUBROUTINE MPI_ALLGATHER_T(v0,v1,v2,v3,v4,v5,v6,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        <type1> v3<dims1>
        INTEGER v4
        INTEGER v5
        INTEGER v6
        INTEGER ierror
        EXTERNAL MPI_ALLGATHER
        CALL MPI_ALLGATHER(v0,v1,v2,v3,v4,v5,v6,ierror)
        END SUBROUTINE MPI_ALLGATHER_T

        SUBROUTINE MPI_GET_ADDRESS_T(v0,v1,ierror)
        <type> v0<dims>
        INTEGER(KIND=MPI_ADDRESS_KIND) v1
        INTEGER ierror
        EXTERNAL MPI_GET_ADDRESS
        CALL MPI_GET_ADDRESS(v0,v1,ierror)
        END SUBROUTINE MPI_GET_ADDRESS_T

        SUBROUTINE MPI_EXSCAN_T(v0,v1,v2,v3,v4,v5,ierror)
        <type> v0<dims>
        <type1> v1<dims1>
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER ierror
        EXTERNAL MPI_EXSCAN
        CALL MPI_EXSCAN(v0,v1,v2,v3,v4,v5,ierror)
        END SUBROUTINE MPI_EXSCAN_T

        SUBROUTINE MPI_PACK_EXTERNAL_T(v0,v1,v2,v3,v4,v5,v6,ierror)
        CHARACTER (LEN=*) v0
        <type> v1<dims>
        INTEGER v2
        INTEGER v3
        <type1> v4<dims1>
        INTEGER(KIND=MPI_ADDRESS_KIND) v5
        INTEGER(KIND=MPI_ADDRESS_KIND) v6
        INTEGER ierror
        EXTERNAL MPI_PACK_EXTERNAL
        CALL MPI_PACK_EXTERNAL(v0,v1,v2,v3,v4,v5,v6,ierror)
        END SUBROUTINE MPI_PACK_EXTERNAL_T

        SUBROUTINE MPI_IRECV_T(v0,v1,v2,v3,v4,v5,v6,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER v6
        INTEGER ierror
        EXTERNAL MPI_IRECV
        CALL MPI_IRECV(v0,v1,v2,v3,v4,v5,v6,ierror)
        END SUBROUTINE MPI_IRECV_T

        SUBROUTINE MPI_WIN_CREATE_KEYVAL_T(v0,v1,v2,v3,ierror)
        EXTERNAL v0
        EXTERNAL v1
        INTEGER v2
        <type> v3<dims>
        INTEGER ierror
        EXTERNAL MPI_WIN_CREATE_KEYVAL
        CALL MPI_WIN_CREATE_KEYVAL(v0,v1,v2,v3,ierror)
        END SUBROUTINE MPI_WIN_CREATE_KEYVAL_T

        SUBROUTINE MPI_SCATTERV_T(v0,v1,v2,v3,v4,v5,v6,v7,v8,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        <type1> v4<dims1>
        INTEGER v5
        INTEGER v6
        INTEGER v7
        INTEGER v8
        INTEGER ierror
        EXTERNAL MPI_SCATTERV
        CALL MPI_SCATTERV(v0,v1,v2,v3,v4,v5,v6,v7,v8,ierror)
        END SUBROUTINE MPI_SCATTERV_T

        SUBROUTINE MPI_SCAN_T(v0,v1,v2,v3,v4,v5,ierror)
        <type> v0<dims>
        <type1> v1<dims1>
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER ierror
        EXTERNAL MPI_SCAN
        CALL MPI_SCAN(v0,v1,v2,v3,v4,v5,ierror)
        END SUBROUTINE MPI_SCAN_T

        SUBROUTINE MPI_RSEND_T(v0,v1,v2,v3,v4,v5,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER ierror
        EXTERNAL MPI_RSEND
        CALL MPI_RSEND(v0,v1,v2,v3,v4,v5,ierror)
        END SUBROUTINE MPI_RSEND_T

        SUBROUTINE MPI_PACK_T(v0,v1,v2,v3,v4,v5,v6,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        <type1> v3<dims1>
        INTEGER v4
        INTEGER v5
        INTEGER v6
        INTEGER ierror
        EXTERNAL MPI_PACK
        CALL MPI_PACK(v0,v1,v2,v3,v4,v5,v6,ierror)
        END SUBROUTINE MPI_PACK_T

        SUBROUTINE MPI_ALLGATHERV_T(v0,v1,v2,v3,v4,v5,v6,v7,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        <type1> v3<dims1>
        INTEGER v4
        INTEGER v5
        INTEGER v6
        INTEGER v7
        INTEGER ierror
        EXTERNAL MPI_ALLGATHERV
        CALL MPI_ALLGATHERV(v0,v1,v2,v3,v4,v5,v6,v7,ierror)
        END SUBROUTINE MPI_ALLGATHERV_T

        SUBROUTINE MPI_BSEND_INIT_T(v0,v1,v2,v3,v4,v5,v6,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER v6
        INTEGER ierror
        EXTERNAL MPI_BSEND_INIT
        CALL MPI_BSEND_INIT(v0,v1,v2,v3,v4,v5,v6,ierror)
        END SUBROUTINE MPI_BSEND_INIT_T

        SUBROUTINE MPI_RSEND_INIT_T(v0,v1,v2,v3,v4,v5,v6,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER v6
        INTEGER ierror
        EXTERNAL MPI_RSEND_INIT
        CALL MPI_RSEND_INIT(v0,v1,v2,v3,v4,v5,v6,ierror)
        END SUBROUTINE MPI_RSEND_INIT_T

        SUBROUTINE MPI_GATHERV_T(v0,v1,v2,v3,v4,v5,v6,v7,v8,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        <type1> v3<dims1>
        INTEGER v4
        INTEGER v5
        INTEGER v6
        INTEGER v7
        INTEGER v8
        INTEGER ierror
        EXTERNAL MPI_GATHERV
        CALL MPI_GATHERV(v0,v1,v2,v3,v4,v5,v6,v7,v8,ierror)
        END SUBROUTINE MPI_GATHERV_T

        SUBROUTINE MPI_REDUCE_SCATTER_BLOCK_T(v0,v1,v2,v3,v4,v5,ierror)
        <type> v0<dims>
        <type1> v1<dims1>
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER ierror
        EXTERNAL MPI_REDUCE_SCATTER_BLOCK
        CALL MPI_REDUCE_SCATTER_BLOCK(v0,v1,v2,v3,v4,v5,ierror)
        END SUBROUTINE MPI_REDUCE_SCATTER_BLOCK_T

        SUBROUTINE MPI_ADDRESS_T(v0,v1,ierror)
        <type> v0<dims>
        INTEGER(KIND=MPI_ADDRESS_KIND) v1
        INTEGER ierror
        EXTERNAL MPI_ADDRESS
        CALL MPI_ADDRESS(v0,v1,ierror)
        END SUBROUTINE MPI_ADDRESS_T

        SUBROUTINE MPI_WIN_GET_ATTR_T(v0,v1,v2,v3,ierror)
        INTEGER v0
        INTEGER v1
        <type> v2<dims>
        LOGICAL v3
        INTEGER ierror
        EXTERNAL MPI_WIN_GET_ATTR
        CALL MPI_WIN_GET_ATTR(v0,v1,v2,v3,ierror)
        END SUBROUTINE MPI_WIN_GET_ATTR_T

        SUBROUTINE MPI_SEND_INIT_T(v0,v1,v2,v3,v4,v5,v6,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER v6
        INTEGER ierror
        EXTERNAL MPI_SEND_INIT
        CALL MPI_SEND_INIT(v0,v1,v2,v3,v4,v5,v6,ierror)
        END SUBROUTINE MPI_SEND_INIT_T

        SUBROUTINE MPI_ALLTOALLW_T(v0,v1,v2,v3,v4,v5,v6,v7,v8,ierror)
        <type> v0<dims>
        INTEGER v1(*)
        INTEGER v2(*)
        INTEGER v3(*)
        <type1> v4<dims1>
        INTEGER v5(*)
        INTEGER v6(*)
        INTEGER v7(*)
        INTEGER v8
        INTEGER ierror
        EXTERNAL MPI_ALLTOALLW
        CALL MPI_ALLTOALLW(v0,v1,v2,v3,v4,v5,v6,v7,v8,ierror)
        END SUBROUTINE MPI_ALLTOALLW_T

        SUBROUTINE MPI_ATTR_GET_T(v0,v1,v2,v3,ierror)
        INTEGER v0
        INTEGER v1
        <type> v2<dims>
        LOGICAL v3
        INTEGER ierror
        EXTERNAL MPI_ATTR_GET
        CALL MPI_ATTR_GET(v0,v1,v2,v3,ierror)
        END SUBROUTINE MPI_ATTR_GET_T

        SUBROUTINE MPI_COMM_SET_ATTR_T(v0,v1,v2,ierror)
        INTEGER v0
        INTEGER v1
        <type> v2<dims>
        INTEGER ierror
        EXTERNAL MPI_COMM_SET_ATTR
        CALL MPI_COMM_SET_ATTR(v0,v1,v2,ierror)
        END SUBROUTINE MPI_COMM_SET_ATTR_T

        SUBROUTINE MPI_SENDRECV_REPLACE_T(v0,v1,v2,v3,v4,v5,v6,v7,v8,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER v6
        INTEGER v7
        INTEGER v8(MPI_STATUS_SIZE)
        INTEGER ierror
        EXTERNAL MPI_SENDRECV_REPLACE
        CALL MPI_SENDRECV_REPLACE(v0,v1,v2,v3,v4,v5,v6,v7,v8,ierror)
        END SUBROUTINE MPI_SENDRECV_REPLACE_T

        SUBROUTINE MPI_TYPE_SET_ATTR_T(v0,v1,v2,ierror)
        INTEGER v0
        INTEGER v1
        <type> v2<dims>
        INTEGER ierror
        EXTERNAL MPI_TYPE_SET_ATTR
        CALL MPI_TYPE_SET_ATTR(v0,v1,v2,ierror)
        END SUBROUTINE MPI_TYPE_SET_ATTR_T

        SUBROUTINE MPI_ISSEND_T(v0,v1,v2,v3,v4,v5,v6,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER v6
        INTEGER ierror
        EXTERNAL MPI_ISSEND
        CALL MPI_ISSEND(v0,v1,v2,v3,v4,v5,v6,ierror)
        END SUBROUTINE MPI_ISSEND_T

        SUBROUTINE MPI_BCAST_T(v0,v1,v2,v3,v4,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER ierror
        EXTERNAL MPI_BCAST
        CALL MPI_BCAST(v0,v1,v2,v3,v4,ierror)
        END SUBROUTINE MPI_BCAST_T

        SUBROUTINE MPI_COMM_GET_ATTR_T(v0,v1,v2,v3,ierror)
        INTEGER v0
        INTEGER v1
        <type> v2<dims>
        LOGICAL v3
        INTEGER ierror
        EXTERNAL MPI_COMM_GET_ATTR
        CALL MPI_COMM_GET_ATTR(v0,v1,v2,v3,ierror)
        END SUBROUTINE MPI_COMM_GET_ATTR_T

        SUBROUTINE MPI_ALLTOALLV_T(v0,v1,v2,v3,v4,v5,v6,v7,v8,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        <type1> v4<dims1>
        INTEGER v5
        INTEGER v6
        INTEGER v7
        INTEGER v8
        INTEGER ierror
        EXTERNAL MPI_ALLTOALLV
        CALL MPI_ALLTOALLV(v0,v1,v2,v3,v4,v5,v6,v7,v8,ierror)
        END SUBROUTINE MPI_ALLTOALLV_T

        SUBROUTINE MPI_ACCUMULATE_T(v0,v1,v2,v3,v4,v5,v6,v7,v8,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER(KIND=MPI_ADDRESS_KIND) v4
        INTEGER v5
        INTEGER v6
        INTEGER v7
        INTEGER v8
        INTEGER ierror
        EXTERNAL MPI_ACCUMULATE
        CALL MPI_ACCUMULATE(v0,v1,v2,v3,v4,v5,v6,v7,v8,ierror)
        END SUBROUTINE MPI_ACCUMULATE_T

        SUBROUTINE MPI_IRSEND_T(v0,v1,v2,v3,v4,v5,v6,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER v6
        INTEGER ierror
        EXTERNAL MPI_IRSEND
        CALL MPI_IRSEND(v0,v1,v2,v3,v4,v5,v6,ierror)
        END SUBROUTINE MPI_IRSEND_T

        SUBROUTINE MPI_COMM_CREATE_KEYVAL_T(v0,v1,v2,v3,ierror)
        EXTERNAL v0
        EXTERNAL v1
        INTEGER v2
        <type> v3<dims>
        INTEGER ierror
        EXTERNAL MPI_COMM_CREATE_KEYVAL
        CALL MPI_COMM_CREATE_KEYVAL(v0,v1,v2,v3,ierror)
        END SUBROUTINE MPI_COMM_CREATE_KEYVAL_T

        SUBROUTINE MPI_SCATTER_T(v0,v1,v2,v3,v4,v5,v6,v7,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        <type1> v3<dims1>
        INTEGER v4
        INTEGER v5
        INTEGER v6
        INTEGER v7
        INTEGER ierror
        EXTERNAL MPI_SCATTER
        CALL MPI_SCATTER(v0,v1,v2,v3,v4,v5,v6,v7,ierror)
        END SUBROUTINE MPI_SCATTER_T

        SUBROUTINE MPI_RECV_INIT_T(v0,v1,v2,v3,v4,v5,v6,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER v6
        INTEGER ierror
        EXTERNAL MPI_RECV_INIT
        CALL MPI_RECV_INIT(v0,v1,v2,v3,v4,v5,v6,ierror)
        END SUBROUTINE MPI_RECV_INIT_T

        SUBROUTINE MPI_GATHER_T(v0,v1,v2,v3,v4,v5,v6,v7,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        <type1> v3<dims1>
        INTEGER v4
        INTEGER v5
        INTEGER v6
        INTEGER v7
        INTEGER ierror
        EXTERNAL MPI_GATHER
        CALL MPI_GATHER(v0,v1,v2,v3,v4,v5,v6,v7,ierror)
        END SUBROUTINE MPI_GATHER_T

        SUBROUTINE MPI_KEYVAL_CREATE_T(v0,v1,v2,v3,ierror)
        EXTERNAL v0
        EXTERNAL v1
        INTEGER v2
        <type> v3<dims>
        INTEGER ierror
        EXTERNAL MPI_KEYVAL_CREATE
        CALL MPI_KEYVAL_CREATE(v0,v1,v2,v3,ierror)
        END SUBROUTINE MPI_KEYVAL_CREATE_T

        SUBROUTINE MPI_REDUCE_T(v0,v1,v2,v3,v4,v5,v6,ierror)
        <type> v0<dims>
        <type1> v1<dims1>
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER v6
        INTEGER ierror
        EXTERNAL MPI_REDUCE
        CALL MPI_REDUCE(v0,v1,v2,v3,v4,v5,v6,ierror)
        END SUBROUTINE MPI_REDUCE_T

        SUBROUTINE MPI_BSEND_T(v0,v1,v2,v3,v4,v5,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER v2
        INTEGER v3
        INTEGER v4
        INTEGER v5
        INTEGER ierror
        EXTERNAL MPI_BSEND
        CALL MPI_BSEND(v0,v1,v2,v3,v4,v5,ierror)
        END SUBROUTINE MPI_BSEND_T

        SUBROUTINE MPI_FREE_MEM_T(v0,ierror)
        <type> v0<dims>
        INTEGER ierror
        EXTERNAL MPI_FREE_MEM
        CALL MPI_FREE_MEM(v0,ierror)
        END SUBROUTINE MPI_FREE_MEM_T

        SUBROUTINE MPI_BUFFER_DETACH_T(v0,v1,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER ierror
        EXTERNAL MPI_BUFFER_DETACH
        CALL MPI_BUFFER_DETACH(v0,v1,ierror)
        END SUBROUTINE MPI_BUFFER_DETACH_T

        SUBROUTINE MPI_BUFFER_ATTACH_T(v0,v1,ierror)
        <type> v0<dims>
        INTEGER v1
        INTEGER ierror
        EXTERNAL MPI_BUFFER_ATTACH
        CALL MPI_BUFFER_ATTACH(v0,v1,ierror)
        END SUBROUTINE MPI_BUFFER_ATTACH_T

        SUBROUTINE MPI_ATTR_PUT_T(v0,v1,v2,ierror)
        INTEGER v0
        INTEGER v1
        <type> v2<dims>
        INTEGER ierror
        EXTERNAL MPI_ATTR_PUT
        CALL MPI_ATTR_PUT(v0,v1,v2,ierror)
        END SUBROUTINE MPI_ATTR_PUT_T

        SUBROUTINE MPI_WIN_SET_ATTR_T(v0,v1,v2,ierror)
        INTEGER v0
        INTEGER v1
        <type> v2<dims>
        INTEGER ierror
        EXTERNAL MPI_WIN_SET_ATTR
        CALL MPI_WIN_SET_ATTR(v0,v1,v2,ierror)
        END SUBROUTINE MPI_WIN_SET_ATTR_T

        END MODULE MPI_t1_s
