local json = require "cjson"
local limit_ub = 500

local function split_uri(uri)
    local parts = {}
    for part in uri:gmatch("[^/]+") do
        table.insert(parts, part)
    end
    return parts
end

local function is_get_single_object(uri)
    local parts = split_uri(uri)
    local first_part = nil
    local num_parts = #parts
    if num_parts > 0 then
        first_part = parts[1]
    end
    if first_part == "api" and num_parts % 2 == 0 then
        return true
    elseif first_part == "apis" and num_parts % 2 == 1 then
        return true
    end
    return false
end

local function is_from_cl2(headers)
    local user = headers["User-Agent"]
    if user and string.sub(user, 1, 13) == "clusterloader" then
        return true
    end
    return false
end

local function decompress_gzip(compressed_data)
    local stream = zlib.inflate()
    local decompressed, err = stream(compressed_data, "finish")
    if not decompressed then
        ngx.log(ngx.ERR, "Gzip decompression failed: ", err)
        return nil
    end
    return decompressed
end

local function call_k8s_api(uri, headers, args)
    local query = ngx.encode_args(args)
    local res, err = ngx.location.capture(uri, {
        headers = headers,
        args = query
    })
    if not res then
        ngx.log(ngx.ERR, "call_k8s_api() error: ", err)
        return nil, nil
    end
    local body = res.body
    if res.header["Content-Encoding"] == "gzip" then
        local decompressed_body = decompress_gzip(body)
        if not decompressed_body then
            return nil, nil
        end
        body = decompressed_body
    end
    if res.status ~= 200 then
        ngx.say(body)
        return res.status, nil
    end
    return res.status, json.decode(body)
end

local function get_obj_nums(uri, headers, args)  -- GET only 1 object to get the total number of objects in a lightweight manner
    args.limit = "1"
    if args.resourceVersion == "0" then
        args.resourceVersion = nil
    end
    local status, response = call_k8s_api(uri, headers, args)
    if response == nil then
        return nil
    end
    if response.items == nil then
        return nil
    end
    local items = response.items
    if #items == 0 then
        return 0
    end
    local remain_items_count = response.metadata.remainingItemCount
    if remain_items_count then
        return 1 + remain_items_count
    else
        return 1
    end
end

local function handle_list_with_large_limit(uri, headers, args)
    local result = {}
    local all_obj = {}
    local continue_token = args.continue
    local remaining_item_count = 0
    local counter = 0  -- loop counter
    local offset = 0
    local status = 200
    local req_limit = tonumber(args.limit)

    if req_limit == nil then
        ngx.log(ngx.WARN, 'request has no limit')
        return nil, nil
    end

    while offset < req_limit do
        local limit_temp = limit_ub
        if req_limit - offset < limit_temp then
            limit_temp = req_limit - offset
        end
        local query = {
            limit = limit_temp,
            continue = continue_token
        }
        local api_result = nil
        status, api_result = call_k8s_api(uri, headers, query)
        if not api_result then
            return status, nil
        end
        if counter == 0 then
            result.kind = api_result.kind
            result.apiVersion = api_result.apiVersion
            result.metadata = {}
            result.metadata.resourceVersion = api_result.metadata.resourceVersion
            result.items = {}
        end
        for _, obj in ipairs(api_result.items) do
            table.insert(all_obj, obj)
        end
        continue_token = api_result.metadata.continue
        remaining_item_count = api_result.metadata.remainingItemCount
        if continue_token == nil then
            break
        end
        counter = counter + 1
        offset = offset + limit_ub
    end

    result.items = all_obj

    if offset >= req_limit then -- some objects still remain
        result.metadata.continue = continue_token
        if remaining_item_count and remaining_item_count > 0 then
            result.metadata.remainingItemCount = remaining_item_count
        end
    end

    return status, json.encode(result)
end

local function handle_list_request(uri, headers, args)
    local result = {}
    local all_objects = {}
    local continue_token = nil
    local counter = 0
    local status = 200

    while true do
        local query = {
            limit = limit_ub,
            continue = continue_token
        }
        local api_result = nil
        status, api_result = call_k8s_api(uri, headers, query)
        if not api_result then
            return status, nil
        end
        if counter == 0 then
            result.kind = api_result.kind
            result.apiVersion = api_result.apiVersion
            result.metadata = {}
            result.metadata.resourceVersion = api_result.metadata.resourceVersion
            result.items = {}
        end
        for _, obj in ipairs(api_result.items) do
            table.insert(all_objects, obj)
        end
        continue_token = api_result.metadata.continue or nil
        if not continue_token then
            break
        end
        counter = counter + 1
    end

    result.items = all_objects

    return status, json.encode(result)
end

-- main
local action = ngx.var.request_method

if action == "GET" then
    local uri = ngx.var.uri
    local headers = ngx.req.get_headers()
    local args = ngx.req.get_uri_args()
    if args.watch == nil and not is_from_cl2(headers) then  -- do nothing for watch requests and requests from clusterloader
        if is_get_single_object(uri) then  -- get single object
            if args["resourceVersion"] == nil then
                args["resourceVersion"] = "0"
                ngx.req.set_uri_args(args)  -- automatically transfer to backend
            end
        elseif args["labelSelector"] == nil then -- list without label selector
            if args.limit == nil then  -- without limit
                local obj_num = get_obj_nums(uri, headers, args)
                -- ngx.log(ngx.ERR, "obj_num: ", obj_num)
                if obj_num == nil then
                    -- do nothing
                elseif obj_num <= limit_ub then
                    if args["resourceVersion"] == nil then
                        args["resourceVersion"] = "0"
                        ngx.req.set_uri_args(args)
                    end
                else  -- obj_num > 500
                    local status, response = handle_list_request(uri, headers, args)
                    ngx.status = status
                    ngx.say(response)
                end
            elseif tonumber(args.limit) > limit_ub then  -- list with limit > limit_ub
                local status, response = handle_list_with_large_limit(uri, headers, args)
                ngx.status = status
                ngx.say(response)
            end
        end
    end
end