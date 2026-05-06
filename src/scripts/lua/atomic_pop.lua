-- Atomically pop up to N tasks from the sorted set whose score <= now.
-- KEYS[1]  = sorted set name  (e.g. "task_scheduler:pending")
-- ARGV[1]  = current unix timestamp (seconds, as string)
-- ARGV[2]  = max number of tasks to pop (batch size)
--
-- Returns a flat list: {task_id, score, task_id, score, ...}
-- or an empty table if nothing is due.

local results = {}
local now   = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])

local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'WITHSCORES', 'LIMIT', 0, limit)

if #items == 0 then
    return results
end

-- Build the removal list (every other element is the member, not the score)
local members = {}
for i = 1, #items, 2 do
    table.insert(members, items[i])
end

-- Remove all matched members in one call
redis.call('ZREM', KEYS[1], unpack(members))

-- Return flat list of {id, score} pairs
for i = 1, #items do
    table.insert(results, items[i])
end

return results
