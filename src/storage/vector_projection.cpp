#include "storage/vector_projection.h"
#include "storage/vector.h"

namespace smartid {

uint64_t VectorProjection::NumRows() const {
  return vectors_.empty() ? 0 : vectors_[0]->NumElems();
}

}