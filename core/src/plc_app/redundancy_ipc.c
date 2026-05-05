#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "redundancy_ipc.h"

#include <stdio.h>
#include <string.h>

#include "../drivers/plugin_driver.h"
#include "plc_state_manager.h"
#include "utils/log.h"

extern plugin_driver_t *plugin_driver;

void redundancy_shadow_exit_response(char *response, size_t response_size)
{
    PyGILState_STATE gstate;
    int gil_held = 0;

    if (!response || response_size == 0)
    {
        return;
    }
    response[0]                 = '\0';
    response[response_size - 1] = '\0';

    if (!plugin_driver)
    {
        snprintf(response, response_size, "REDUNDANCY_SHADOW_EXIT:NO_DRIVER\n");
        return;
    }

    if (!plugin_driver->shadow_standby)
    {
        snprintf(response, response_size, "REDUNDANCY_SHADOW_EXIT:NOT_SHADOW\n");
        return;
    }

    if (plc_get_state() != PLC_STATE_RUNNING)
    {
        snprintf(response, response_size, "REDUNDANCY_SHADOW_EXIT:NOT_RUNNING\n");
        return;
    }

    if (Py_IsInitialized())
    {
        gstate   = PyGILState_Ensure();
        gil_held = 1;
    }

    plugin_driver_stop(plugin_driver);

    plugin_driver_set_shadow_standby(plugin_driver, 0);

    if (plugin_driver_load_config(plugin_driver, "./plugins.conf") != 0)
    {
        log_error("[REDUNDANCY]: Shadow exit failed at load_config");
        snprintf(response, response_size, "REDUNDANCY_SHADOW_EXIT:LOAD_CONFIG_FAILED\n");
        plugin_driver_set_shadow_standby(plugin_driver, 1);
        goto out;
    }

    if (plugin_driver_init(plugin_driver) != 0)
    {
        log_error("[REDUNDANCY]: Shadow exit failed at plugin init");
        snprintf(response, response_size, "REDUNDANCY_SHADOW_EXIT:INIT_FAILED\n");
        goto out;
    }

    if (plugin_driver_start(plugin_driver) != 0)
    {
        log_error("[REDUNDANCY]: Shadow exit failed at plugin start");
        snprintf(response, response_size, "REDUNDANCY_SHADOW_EXIT:START_FAILED\n");
        goto out;
    }

    log_info("[REDUNDANCY]: Shadow standby exited — field I/O plugins active");
    snprintf(response, response_size, "REDUNDANCY_SHADOW_EXIT:OK\n");

out:
    if (gil_held)
    {
        PyGILState_Release(gstate);
    }
}
