-- KEYS: partition checkpoints, current ticket state, queue counters, outcome counters.
-- ARGV: partition, record offset, expected checkpoint, id, version, queue, status.
local offsets, tickets, counts, outcomes = unpack(KEYS)
local part, offset, expected, id, version, queue, status = unpack(ARGV)
local stored = tonumber(redis.call('HGET', offsets, part) or '0')
if tonumber(offset) < stored then return 'already_applied' end
if stored ~= tonumber(expected) then return redis.error_reply('CHECKPOINT_CHANGED') end

local outcome = 'invalid'
if id ~= '' then
    local previous = redis.call('HGET', tickets, id)
    local old_version, old_queue, old_status
    if previous then
        old_version, old_queue, old_status = string.match(previous, '^(%d+)|([^|]+)|([^|]+)$')
    end
    if previous and tonumber(version) < tonumber(old_version) then
        outcome = 'stale'
    elseif previous and tonumber(version) == tonumber(old_version) then
        outcome = old_queue == queue and old_status == status and 'duplicate' or 'conflict'
    else
        if previous then redis.call('HINCRBY', counts, old_queue .. '|' .. old_status, -1) end
        redis.call('HINCRBY', counts, queue .. '|' .. status, 1)
        redis.call('HSET', tickets, id, version .. '|' .. queue .. '|' .. status)
        outcome = 'applied'
    end
end
redis.call('HINCRBY', outcomes, outcome, 1)
redis.call('HSET', offsets, part, string.format('%.0f', tonumber(offset) + 1))
return outcome
