#include <pybind11/pybind11.h>

#include <lakesoul/add.h>
#include <lakesoul/example.h>

std::string hello_from_bin() { return "Hello from lakesoul-dataset!"; }

namespace py = pybind11;

PYBIND11_MODULE(_core, m) {
  m.doc() = "pybind11 hello module";

  m.def("hello_from_bin", &hello_from_bin, R"pbdoc(
      A function that returns a Hello string.
  )pbdoc");

  m.def("add", &lakesoul::add);
  m.def("arrow_main", &lakesoul::arrow_main);
}
