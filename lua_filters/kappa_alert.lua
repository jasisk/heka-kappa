require("string")
local alert = require "alert"

local host_counters = {}
local host_count = 0

function process_message()
    local host = read_message("Fields[host]")
    local count = read_message("Fields[count]")
    local hc = host_counters[host]
    if hc == nil then
        hc = count
        host_count = host_count + 1
	host_counters[host] = hc
    else
        host_counters[host] = hc + count
    end
    return 0
end

function timer_event(ns)
    if host_count > 0 then
        add_to_payload("Kappa instances are unstable!\n")
        for k,v in pairs(host_counters) do
            add_to_payload(string.format("*%s* has seen *%d* errors in the last *5* minutes.\n", k, v))
	    host_counters[k] = nil
        end
        inject_payload("txt", "KappaAlert")
    end
end
