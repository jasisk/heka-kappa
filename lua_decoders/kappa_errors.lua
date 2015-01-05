require "cjson"

local type = read_config("type")

function map(array)
    local new_array = {}
    for i,v in ipairs(array) do
        new_array[v[3]] = v[2] -- should probably use the table lib here
    end

    return new_array
end

function process_message()
    local raw_message = read_message("Payload")
    local ok, json = pcall(cjson.decode, raw_message)

    if not ok then
        return 0
    end

    if json[1] ~= nil then
	local points = json[1]["points"]
        local hosts = map(points)
        for host,count in pairs(hosts) do
            local m = {
                Type = type,
                Payload = host .. ": " .. count,
                Fields = {
                    host = host,
                    count = count
                }
            }
            if not pcall(inject_message, m) then return -1 end
        end
    end

    return 0
end

