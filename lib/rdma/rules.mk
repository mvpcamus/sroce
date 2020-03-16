include mk/subdir_pre.mk

LIB_RDMA_OBJS := $(addprefix $(d)/, control.o dataops.o)
LIB_RDMA_SOBJS := $(LIB_RDMA_OBJS:.o=.shared.o)

LIB_RDMA_CPPFLAGS := -I$(d)/include/ -Ilib/tas/include/

$(LIB_RDMA_OBJS): CPPFLAGS += $(LIB_RDMA_CPPFLAGS)
$(LIB_RDMA_SOBJS): CPPFLAGS += $(LIB_RDMA_CPPFLAGS)

lib/libtas_rdma.so: $(LIB_RDMA_SOBJS) $(LIB_TAS_SOBJS) $(LIB_UTILS_SOBJS)

DEPS += $(LIB_RDMA_OBJS:.o=.d) $(LIB_RDMA_SOBJS:.o=.d)
CLEAN += $(LIB_RDMA_OBJS) lib/libtas_rdma.so
TARGETS += lib/libtas_rdma.so

include mk/subdir_post.mk
