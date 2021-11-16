namespace smartid::ops {
template <typename T>
struct Add {
  static T Apply(const T& in1, const T& in2) {
    return in1 + in2;
  }
};

template <typename T>
struct Mul {
  static T Apply(const T& in1, const T& in2) {
    return in1 * in2;
  }
};

template <typename T>
struct Sub {
  static T Apply(const T& in1, const T& in2) {
    return in1 - in2;
  }
};

template <typename T>
struct Div {
  static T Apply(const T& in1, const T& in2) {
    return in1 / in2;
  }
};

template <typename T>
struct Lt {
  static bool Apply(const T& in1, const T& in2) {
    return in1 < in2;
  }
};

template <typename T>
struct Le {
  static bool Apply(const T& in1, const T& in2) {
    return in1 <= in2;
  }
};

template <typename T>
struct Gt {
  static bool Apply(const T& in1, const T& in2) {
    return in1 > in2;
  }
};

template <typename T>
struct Ge {
  static bool Apply(const T& in1, const T& in2) {
    return in1 >= in2;
  }
};

template <typename T>
struct Eq {
  static bool Apply(const T& in1, const T& in2) {
    return in1 == in2;
  }
};

template <typename T>
struct Ne {
  static bool Apply(const T& in1, const T& in2) {
    return in1 != in2;
  }
};
}