list (APPEND CMAKE_PREFIX_PATH /usr/bin)

find_package (Hiactor REQUIRED)

include (/home/liyw/hiactor_codegen/ActorAutoGen.cmake)

hiactor_codegen (actor_autogen actor_gen_files
  SOURCE_DIR $my_app_dir
  INCLUDE_PATHS /home/liyw/hiactor/include,$my_app_dir) 

add_executable (bank_account_example
  main.cc
  actor/bank_account.act.cc
  actor/bank_account.act.h
  ${actor_gen_files})

# Disable attribute warning
target_compile_options(bank_account_example
  PRIVATE -Wno-attributes)

add_dependencies (bank_account_example
  actor_autogen)

target_link_libraries (bank_account_example
  PRIVATE Hiactor::hiactor)