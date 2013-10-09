make -j 40 build/bin/units_access
find test/autojson -iname "*.json" ! -name "select_project_materialize_parallel.json" | xargs rm
! ./build/units_access --gtest_filter="*AutoJson*" | grep " FAILED "
