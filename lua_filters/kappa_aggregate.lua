local annotation = require "annotation"
local anomaly = require "anomaly"
local alert = require "alert"
local cjson = require "cjson"
require "circular_buffer"
require "string"
require "table"

local hosts = {}
local cbufs = {}
local titles = {}
local configs = {}
local hosts_size = 0

local rows = read_config("rows") or 120 -- 2 hours at 1 minute
local sec_per_row = read_config("sec_per_row") or 60 -- 1 minute
local alert_throttle = read_config("alert_throttle") or 5 * 60 * 1e9 -- 5 mins
local anomaly_config = read_config("anomaly_config") or error("anomaly_config required")

alert.set_throttle(alert_throttle)

local function init_cb()

    local cb = circular_buffer.new(rows, 6, sec_per_row)
    for i=1,5 do cb:set_header(i, i*100) end
    cb:set_header(6, "Unknown")

    return cb
end

function process_message ()
    local ts = read_message("Timestamp")
    local hostname = read_message("Hostname")
    local status = read_message("Fields[status]")
    local count = read_message("Fields[count]")

    local host = hosts[hostname]

    if not host then
        hosts_size = hosts_size + 1
        hosts[hostname] = {last_update = ts, index = hosts_size}
        titles[hosts_size] = string.format("%s http status", hostname)
        configs[hosts_size] = anomaly.parse_config(anomaly_config)
        annotation.set_prune(titles[hosts_size], rows * sec_per_row * 1e9)
        host = hosts[hostname]
    end

    local statuses = cbufs[host.index]

    if not statuses then
        cbufs[host.index] = init_cb()
        statuses = cbufs[host.index]
    end

    local col = status/100
    if col >= 1 and col < 6 then
        statuses:add(ts, col, count) -- col will be truncated to an int
    else
        statuses:add(ts, 6, count)
    end

    return 0
end

function timer_event(ns)
    for host, meta in pairs(hosts) do
        local cbuf = cbufs[meta.index]
        local title = titles[meta.index]
        local aconf = configs[meta.index]

        for i=1,6 do
            cbuf:add(ns, i, 0) -- ensure columns are zeroed out
        end

        if aconf then
            if not alert.throttled(ns) then
                for key, array in pairs(aconf) do
                    for i, cfg in ipairs(array) do
                        local comp_range = (cfg.win * cfg.nwin + 1) * sec_per_row * 1e9
                        local row = cbuf:get(ns - comp_range, cfg.col)
                        if not (row ~= row) then
                            local msg, annos = anomaly.detect(ns, key, cbuf, aconf)
                            if msg then
                                local sum, rows = cbuf:compute("sum", cfg.col, ns - 5 * 60 * 1e9)
                                if sum then
                                    annotation.concat(title, annos)
                                    alert.queue(ns, string.format("*%s* has seen *%d* %s statuses in the last *%d* minutes", host, sum, key, 5))
                                end
                            end
                        end
                    end
                end
            end
            inject_payload("cbuf", title, annotation.prune(title, ns), cbuf)
        else
            inject_payload("cbuf", title, cbuf)
        end
    end
    alert.send_queue(ns)
end
