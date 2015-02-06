local annotation = require "annotation"
local anomaly = require "anomaly"
local alert = require "alert"
require "circular_buffer"
require "string"

local hosts = {}
local cbufs = {}
local titles = {}
local hosts_size = 0

local rows = read_config("rows") or 288 -- 24 hours @ 5 mins
local sec_per_row = read_config("sec_per_row") or 300 -- 5 mins
local alert_throttle = read_config("alert_throttle") or 5 * 60 * 1e9 -- 5 mins
local anomaly_config = anomaly.parse_config(read_config("anomaly_config"))

alert.set_throttle(alert_throttle)

function init_cb()

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
        if anomaly_config then
            if not alert.throttled(ns) then
                local msg, annos = anomaly.detect(ns, title, cbuf, anomaly_config)
                if msg then
                    annotation.concat(title, annos)
                    local sum_400, rows_400 = cbuf:compute("sum", 4, ns - (5 * 60 * 1e9))
                    local sum_500, rows_500 = cbuf:compute("sum", 5, ns - (5 * 60 * 1e9))
                    alert.queue(ns, string.format("*%s* has seen *%s* 4xx, *%s* 5xx statuses in the last *5* minutes", host, sum_400, sum_500))
                end
            end
            inject_payload("cbuf", title, annotation.prune(title, ns), cbuf)
        else
            inject_payload("cbuf", title, cbuf)
        end
    end
    alert.send_queue(ns)
end
