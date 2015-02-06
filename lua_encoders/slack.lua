require "cjson"

local message = {
    text = nil,
    username = "kappabot",
    icon_emoji = ":ghost:"
}

local min_severity = read_config("min_severity") or 8

function process_message()
    local severity = read_message("Severity")
    local msg = read_message("Payload")

    if severity <= min_severity then msg = "<!channel>: " .. msg end

    message.text = msg

    add_to_payload(cjson.encode(message))
    inject_payload()

    return 0
end
