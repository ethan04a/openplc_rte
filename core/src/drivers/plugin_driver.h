#ifndef PLUGIN_DRIVER_H
#define PLUGIN_DRIVER_H

#include "../plc_app/plcapp_manager.h"
#include "plugin_config.h"
#include "plugin_types.h"
#include "python_plugin_bridge.h"

// Maximum number of plugins
#define MAX_PLUGINS 16

typedef enum
{
    PLUGIN_TYPE_PYTHON,
    PLUGIN_TYPE_NATIVE
} plugin_type_t;

typedef int (*plugin_init_func_t)(void *);
typedef int (*plugin_start_loop_func_t)(void);
typedef void (*plugin_stop_loop_func_t)(void);
typedef void (*plugin_cycle_start_func_t)(void);
typedef void (*plugin_cycle_end_func_t)(void);
typedef void (*plugin_cleanup_func_t)(void);

typedef struct
{
    void *handle; // Handle to the loaded shared library
    plugin_init_func_t init;
    plugin_start_loop_func_t start;
    plugin_stop_loop_func_t stop;
    plugin_cycle_start_func_t cycle_start;
    plugin_cycle_end_func_t cycle_end;
    plugin_cleanup_func_t cleanup;
} plugin_funct_bundle_t;

// Plugin instance structure
typedef struct plugin_instance_s
{
    PluginManager *manager;
    python_binds_t *python_plugin;
    plugin_funct_bundle_t *native_plugin;
    // pthread_t thread;
    int running;
    plugin_config_t config;
} plugin_instance_t;

// Driver structure
typedef struct
{
    plugin_instance_t plugins[MAX_PLUGINS];
    int plugin_count;
    pthread_mutex_t buffer_mutex;
    /** When non-zero: redundancy standby shadow mode — no field I/O plugins loaded or started. */
    int shadow_standby;
} plugin_driver_t;

// Driver management functions
plugin_driver_t *plugin_driver_create(void);
void plugin_driver_set_shadow_standby(plugin_driver_t *driver, int enabled);
int plugin_driver_load_config(plugin_driver_t *driver, const char *config_file);
int plugin_driver_update_config(plugin_driver_t *driver, const char *config_file);
int plugin_driver_init(plugin_driver_t *driver);
int plugin_driver_start(plugin_driver_t *driver);
int plugin_driver_stop(plugin_driver_t *driver);
void plugin_driver_destroy(plugin_driver_t *driver);
int plugin_mutex_take(pthread_mutex_t *mutex);
int plugin_mutex_give(pthread_mutex_t *mutex);

// Cycle hook functions for native plugins (called during PLC scan cycle)
// These iterate through all active native plugins and call their cycle hooks
// Plugins opt-in by implementing cycle_start/cycle_end; opt-out by not implementing them
void plugin_driver_cycle_start(plugin_driver_t *driver);
void plugin_driver_cycle_end(plugin_driver_t *driver);

// Python plugin functions
int python_plugin_get_symbols(plugin_instance_t *plugin);

// Native plugin functions
int native_plugin_get_symbols(plugin_instance_t *plugin);

// Runtime arguments generation
void *generate_structured_args_with_driver(plugin_type_t type, plugin_driver_t *driver,
                                           int plugin_index);
void free_structured_args(plugin_runtime_args_t *args);

#endif // PLUGIN_DRIVER_H
