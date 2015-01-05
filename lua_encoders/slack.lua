require "cjson"

local message = {
    text = nil,
    username = "kappabot",
    icon_emoji = ":ghost:"
}

function process_message()
    local msg = read_message("Payload")
    msg = "<!channel>: " .. msg
    message.text = msg
    add_to_payload(cjson.encode(message))
    inject_payload()

    return 0
end
