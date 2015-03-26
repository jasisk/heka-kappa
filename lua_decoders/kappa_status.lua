local alert = require("alert")
require "string"
require "cjson"
require "table"

local type = read_config("type")
local previously_error = false

function process_message()
    local raw_message = read_message("Payload")
    local original_type = read_message("Type")

    if original_type == 'heka.httpinput.error' then
        -- get the query out of the payload because it might contain sensitive
        -- information like credentials

        local logger = read_message("Logger")
        logger = string.gsub(logger, "%p", "%%%1")

        local new_payload = string.gsub(raw_message, logger, "[REDACTED]")

        local m = {
            Payload = new_payload,
            Fields = {
                previous_error = previously_error
            }
        }

        inject_message(m) -- change the original payload ...
        previously_error = true

        return 0
    end

    previously_error = false

    local ok, json = pcall(cjson.decode, raw_message)

    if not ok then
        return -1 -- if we can't parse the body, that's a problem
    end

    if json[1] ~= nil then
        local points = json[1]["points"]
        for i, row in ipairs(points) do
            -- time is in milliseconds
            local time, count, host, status = row[1], row[2], row[3], row[4]
            local ok, json = pcall(cjson.encode, row)

            if not ok then
                json = table.concat(row, ':')
            end

            local msg = {
                Type = type,
                Payload = json,
                Hostname = host,
                Timestamp = time * 1e6,
                Fields = {
                    count = count,
                    status = status
                }
            }

            -- inject a new message into the pipeline
            if not pcall(inject_message, msg) then return -1 end
        end
    end

    return 0
end
