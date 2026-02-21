-- MoneyMoney WebBanking Extension: Bitvavo (MVP)
--
-- Architecture Overview
-- - Accounts exposed:
--   - Cash account (`Bitvavo-VK`): EUR settlement balance and EUR cash transactions.
--   - Portfolio account (`Bitvavo-DP`): non-EUR asset positions, optionally EUR-valued.
-- - `RefreshAccount` data flow:
--   - Fetch `/v2/balance` first as the common source for all account paths.
--   - Cash path: build transactions (incremental or full backfill) and return EUR balance.
--   - Portfolio path: map non-EUR balances to securities and enrich with EUR prices.
-- - Bitvavo endpoints and purpose:
--   - `/v2/balance`: current balances (EUR cash + asset quantities).
--   - `/v2/account/history`: incremental account events for cash sync.
--   - `/v2/depositHistory`: EUR deposit backfill for full sync.
--   - `/v2/withdrawalHistory`: EUR withdrawal backfill for full sync.
--   - `/v2/trades`: EUR trade cash legs for full-sync reconstruction.
--   - `/v2/markets`: discover valid EUR markets for trade backfill.
--   - `/v2/ticker/price`: EUR valuation for portfolio securities.
-- - Sync strategy:
--   - Incremental (`since` provided): use `/v2/account/history`.
--   - Full (`since == nil`): run backfill endpoints + opening reconciliation.
-- - Rate-limit protections:
--   - Request throttling, bounded retry/backoff, and per-mode request budgets.
--   - These safeguards keep sync responsive and avoid excessive API pressure.
-- - Security assumptions:
--   - API credentials are read-only; no trading or withdrawals are executed.
--   - Only `https://api.bitvavo.com` is contacted.

-- ---------------------------------------------------------------------------
-- 1) Extension Registration (WebBanking{...})
-- ---------------------------------------------------------------------------

WebBanking{
  version     = 1.0,
  description = "Bitvavo (Balances + Hybrid cash history + EUR valuation)",
  services    = {"Bitvavo"}
}

-- ---------------------------------------------------------------------------
-- 2) Configuration Constants (Rate Limiting, Budgets, Backfill, Account IDs)
-- ---------------------------------------------------------------------------

local BASE_URL = "https://api.bitvavo.com"
local ACCESS_WINDOW_MS = "10000"
local BACKFILL_LIMIT = 1000
local MAX_WINDOW_SPLIT_DEPTH = 20
local ACCOUNT_HISTORY_MAX_ITEMS = 100

-- Sync protections to reduce rate-limit risk during MoneyMoney refreshes.
local REQUEST_MIN_INTERVAL_MS = 350
local MAX_HTTP_RETRIES = 2
local RETRY_BASE_DELAY_MS = 800
local MAX_AUTO_WAIT_ON_429_MS = 15000
-- Per-refresh request budgets by mode (incremental cash, full cash, portfolio).
local REQUEST_BUDGET_INCREMENTAL = 120
local REQUEST_BUDGET_FULLSYNC = 260
local REQUEST_BUDGET_PORTFOLIO = 180

-- Backfill optimization: infer likely EUR markets instead of querying all markets.
local MARKET_DISCOVERY_HISTORY_PAGES = 2
-- Safety cap for full-sync trade backfill requests.
-- Limits how many EUR markets are queried via /v2/trades after candidate selection.
-- Keep this conservative to reduce runtime and rate-limit pressure.
local MAX_BACKFILL_MARKETS = 12
-- Priority markets are pinned before alphabetical truncation when available.
local PRIORITY_BACKFILL_MARKETS = { "BTC-EUR", "ETH-EUR", "SOL-EUR" }
local CASH_ACCOUNT_NUMBER = "Bitvavo-VK"
local PORTFOLIO_ACCOUNT_NUMBER = "Bitvavo-DP"

-- Emergency switch for testing:
-- true  => ignore MoneyMoney's incremental `since` and force full cash backfill.
-- false => normal incremental behavior.
local FORCE_FULL_SYNC = false
local DEBUG_VALIDATION = false

local apiKey
local apiSecret
local connection
local requestBudgetRemaining = nil
local requestCount = 0
local lastRequestMs = nil

-- ---------------------------------------------------------------------------
-- 3) HTTP/Auth Helpers (Signing, Request Wrapper, Private/Public GET)
-- ---------------------------------------------------------------------------

local function formatApiError(context, err)
  return "Bitvavo API Fehler (" .. context .. "): " .. (err or "unknown")
end

local function toHex(bin)
  return (bin:gsub(".", function(c)
    return string.format("%02x", string.byte(c))
  end))
end

local function nowMs()
  -- MM.time() returns fractional seconds; convert to milliseconds.
  return math.floor(MM.time() * 1000)
end

local function nowSec()
  return math.floor(nowMs() / 1000)
end

local function safeSleep(seconds)
  local waitSeconds = tonumber(seconds) or 0
  if waitSeconds <= 0 then
    return
  end

  if MM ~= nil and type(MM.sleep) == "function" then
    MM.sleep(waitSeconds)
    return
  end

  local target = MM.time() + waitSeconds
  while MM.time() < target do
    -- Busy-wait fallback if MM.sleep is unavailable.
  end
end

local function throttleRequests()
  if REQUEST_MIN_INTERVAL_MS <= 0 then
    return
  end
  local now = nowMs()
  if lastRequestMs ~= nil then
    local elapsed = now - lastRequestMs
    local missing = REQUEST_MIN_INTERVAL_MS - elapsed
    if missing > 0 then
      safeSleep(missing / 1000)
    end
  end
  lastRequestMs = nowMs()
end

local function resetRequestState(budget)
  requestBudgetRemaining = budget
  requestCount = 0
  lastRequestMs = nil
end

local function consumeRequestBudget(context)
  requestCount = requestCount or 0

  if requestBudgetRemaining == nil then
    requestCount = requestCount + 1
    return true, nil
  end
  if requestBudgetRemaining <= 0 then
    return false, "Request-Budget erreicht bei " .. tostring(context) .. " (Anfragen: " .. tostring(requestCount) .. ")"
  end
  requestBudgetRemaining = requestBudgetRemaining - 1
  requestCount = requestCount + 1
  return true, nil
end

-- Purpose: Create the Bitvavo HMAC signature for a private request.
-- Inputs: `tsMs` timestamp in ms, HTTP `method`, request `path`, and raw `body`.
-- Outputs: Lowercase hex SHA-256 HMAC signature string.
-- Assumptions: `apiSecret` is set and inputs match the exact outgoing request.
-- Error behavior: Relies on MM.hmac256; no local error handling is added here.
local function signRequest(tsMs, method, path, body)
  -- Bitvavo signature formula: hex(HMAC_SHA256(secret, timestamp + method + path + body)).
  local payload = tostring(tsMs) .. method .. path .. (body or "")
  local macBin = MM.hmac256(apiSecret, payload) -- Returns binary digest.
  return toHex(macBin)
end

-- Purpose: Execute one HTTP request and decode JSON into dictionary/array.
-- Inputs: HTTP `method`, absolute `url`, request `headers`, optional `body`.
-- Outputs: `(table, nil)` on success or `(nil, errText)` on failure.
-- Assumptions: `connection` exists and returns JSON-compatible payloads.
-- Error behavior: Retries bounded 429 responses with backoff, then returns errors.
local function requestJson(method, url, headers, body)
  local contentType = "application/json"
  local bodyStr = body or ""
  local attempt = 0

  while attempt <= MAX_HTTP_RETRIES do
    throttleRequests()
    local ok, resp = pcall(function()
      return connection:request(method, url, bodyStr, contentType, headers)
    end)

    if ok then
      local json = JSON(resp)
      local dict = json:dictionary()
      if dict ~= nil then
        return dict, nil
      end
      local arr = json:array()
      if arr ~= nil then
        return arr, nil
      end
      return nil, "Unexpected JSON response"
    end

    local errText = tostring(resp)
    local is429 = errText:find("429", 1, true) ~= nil
    if is429 and attempt < MAX_HTTP_RETRIES then
      local waitMs
      local banExpiryMs = tonumber(errText:match("expires at (%d+)"))
      if banExpiryMs ~= nil then
        waitMs = math.max(0, banExpiryMs - nowMs()) + 1000
      else
        waitMs = RETRY_BASE_DELAY_MS * (2 ^ attempt)
      end

      if waitMs > MAX_AUTO_WAIT_ON_429_MS then
        return nil, "Bitvavo Rate-Limit aktiv; automatisches Warten zu lang (" .. tostring(waitMs) .. "ms). Bitte später erneut versuchen."
      end

      safeSleep(waitMs / 1000)
      attempt = attempt + 1
    else
      return nil, errText
    end
  end

  return nil, "HTTP retry limit exceeded"
end

-- Purpose: Execute an authenticated Bitvavo GET request for private endpoints.
-- Inputs: API `path` including query string (for example `/v2/balance`).
-- Outputs: `(table, nil)` on success or `(nil, errText)` on failure.
-- Assumptions: API key/secret are initialized and budget permits a request.
-- Error behavior: Stops early on budget exhaustion and propagates HTTP/API errors.
local function privateGet(path)
  local okBudget, budgetErr = consumeRequestBudget(path)
  if not okBudget then
    return nil, budgetErr
  end

  local method = "GET"
  local tsMs = nowMs()
  local signature = signRequest(tsMs, method, path, "")

  local headers = {
    Accept = "application/json",
    ["Bitvavo-Access-Key"] = apiKey,
    ["Bitvavo-Access-Timestamp"] = tostring(tsMs),
    ["Bitvavo-Access-Signature"] = signature,
    ["Bitvavo-Access-Window"] = ACCESS_WINDOW_MS
  }

  return requestJson(method, BASE_URL .. path, headers, "")
end

-- Purpose: Execute an unauthenticated GET request for public Bitvavo endpoints.
-- Inputs: Relative `pathWithQuery` (for example `/v2/markets`).
-- Outputs: `(table, nil)` on success or `(nil, errText)` on failure.
-- Assumptions: Endpoint is public and request budget still allows the call.
-- Error behavior: Propagates budget and HTTP/JSON parsing errors unchanged.
local function publicGet(pathWithQuery)
  local okBudget, budgetErr = consumeRequestBudget(pathWithQuery)
  if not okBudget then
    return nil, budgetErr
  end

  local headers = { Accept = "application/json" }
  return requestJson("GET", BASE_URL .. pathWithQuery, headers, "")
end

-- ---------------------------------------------------------------------------
-- 4) Time/Format Helpers
-- ---------------------------------------------------------------------------

local function toNumberSafe(value)
  local n = tonumber(value)
  if n == nil then
    return 0
  end
  return n
end

-- Purpose: Parse ISO-8601 UTC timestamp text into Unix epoch seconds.
-- Inputs: `iso8601` string expected like `YYYY-MM-DDTHH:MM:SS...`.
-- Outputs: Integer epoch seconds on success; `nil` for invalid input.
-- Assumptions: Timestamp is UTC and does not require timezone offsets from input.
-- Error behavior: Returns `nil` on parse/conversion issues; no exception raised.
local function isoToUnixSeconds(iso8601)
  if type(iso8601) ~= "string" then
    return nil
  end

  local y, mo, d, h, mi, s = iso8601:match("^(%d%d%d%d)%-(%d%d)%-(%d%d)T(%d%d):(%d%d):(%d%d)")
  if y == nil then
    return nil
  end

  local year = tonumber(y)
  local month = tonumber(mo)
  local day = tonumber(d)
  local hour = tonumber(h)
  local min = tonumber(mi)
  local sec = tonumber(s)
  if year == nil or month == nil or day == nil or hour == nil or min == nil or sec == nil then
    return nil
  end
  ---@cast year integer
  ---@cast month integer
  ---@cast day integer
  ---@cast hour integer
  ---@cast min integer
  ---@cast sec integer

  local localTs = os.time({
    year = year,
    month = month,
    day = day,
    hour = hour,
    min = min,
    sec = sec,
    isdst = false
  })
  if localTs == nil then
    return nil
  end

  -- Convert a UTC-like table interpreted as local time into a true UTC epoch.
  local utcAsTable = os.date("!*t", localTs)
  if type(utcAsTable) ~= "table" then
    return nil
  end
  utcAsTable.isdst = false
  local utcAsLocalTs = os.time(utcAsTable)
  if utcAsLocalTs == nil then
    return nil
  end

  local offset = localTs - utcAsLocalTs
  return localTs + offset
end

-- ---------------------------------------------------------------------------
-- 5) Validation/Debug Helpers
-- ---------------------------------------------------------------------------

local function debugLog(message)
  if not DEBUG_VALIDATION then
    return
  end
  if type(print) == "function" then
    print("[Bitvavo][DEBUG] " .. tostring(message))
  end
end

local function validationWarn(context, detail)
  debugLog("Validation " .. tostring(context) .. ": " .. tostring(detail))
end

local function validateNumericField(value, context, required)
  if value == nil or value == "" then
    if required then
      validationWarn(context, "missing required numeric field")
    end
    return nil
  end

  local numeric = tonumber(value)
  if numeric == nil then
    validationWarn(context, "non-numeric value (" .. tostring(value) .. ")")
    return nil
  end
  return numeric
end

local function validateTimestampMsToDate(value, context)
  local ms = validateNumericField(value, context, true)
  if ms == nil then
    return
  end
  local unix = math.floor(ms / 1000)
  if os.date("!%Y-%m-%dT%H:%M:%SZ", unix) == nil then
    validationWarn(context, "ms->date conversion failed (" .. tostring(value) .. ")")
  end
end

local function validateEurQuoteMarket(market, context)
  if type(market) ~= "string" or market == "" then
    validationWarn(context, "missing required market")
    return
  end
  local normalized = string.upper(market)
  local separator = normalized:find("-", 1, true)
  if separator == nil or normalized:sub(-4) ~= "-EUR" then
    validationWarn(context, "expected EUR quote market, got " .. tostring(market))
  end
end

local function validateBalancesForDebug(balances, context)
  if not DEBUG_VALIDATION then
    return
  end
  local prefix = context or "/v2/balance"
  if type(balances) ~= "table" then
    validationWarn(prefix, "response is not a table")
    return
  end

  for idx, entry in ipairs(balances) do
    local base = prefix .. "[" .. tostring(idx) .. "]"
    if type(entry) ~= "table" then
      validationWarn(base, "entry is not an object")
    else
      if type(entry.symbol) ~= "string" or entry.symbol == "" then
        validationWarn(base .. ".symbol", "missing required symbol")
      end
      validateNumericField(entry.available, base .. ".available", true)
      validateNumericField(entry.inOrder, base .. ".inOrder", true)
    end
  end
end

local function validateAccountHistoryItemsForDebug(items, context)
  if not DEBUG_VALIDATION then
    return
  end
  local prefix = context or "/v2/account/history.items"
  if type(items) ~= "table" then
    validationWarn(prefix, "items is not a table")
    return
  end

  for idx, item in ipairs(items) do
    local base = prefix .. "[" .. tostring(idx) .. "]"
    if type(item) ~= "table" then
      validationWarn(base, "item is not an object")
    else
      local txType = string.upper(tostring(item.type or ""))
      if txType == "" then
        validationWarn(base .. ".type", "missing transaction type")
      end

      local executedAt = item.executedAt
      if type(executedAt) ~= "string" or isoToUnixSeconds(executedAt) == nil then
        validationWarn(base .. ".executedAt", "invalid ISO-8601 timestamp")
      end

      local sent = string.upper(tostring(item.sentCurrency or ""))
      local received = string.upper(tostring(item.receivedCurrency or ""))
      if sent ~= "" and received ~= "" and sent ~= "EUR" and received ~= "EUR" then
        validationWarn(base, "neither sentCurrency nor receivedCurrency is EUR")
      end
    end
  end
end

local function validateTransferItemsForDebug(items, endpointName)
  if not DEBUG_VALIDATION then
    return
  end
  local prefix = endpointName or "history"
  if type(items) ~= "table" then
    validationWarn(prefix, "response is not a table")
    return
  end

  for idx, item in ipairs(items) do
    local base = prefix .. "[" .. tostring(idx) .. "]"
    if type(item) ~= "table" then
      validationWarn(base, "item is not an object")
    else
      if type(item.symbol) ~= "string" or item.symbol == "" then
        validationWarn(base .. ".symbol", "missing required symbol")
      end
      if item.status == nil then
        validationWarn(base .. ".status", "missing status")
      end
      validateNumericField(item.amount, base .. ".amount", true)
      validateNumericField(item.fee, base .. ".fee", true)
      validateTimestampMsToDate(item.timestamp, base .. ".timestamp")
    end
  end
end

local function validateTradesForDebug(market, trades)
  if not DEBUG_VALIDATION then
    return
  end

  validateEurQuoteMarket(market, "trades.market")
  if type(trades) ~= "table" then
    validationWarn("trades", "response is not a table")
    return
  end

  for idx, trade in ipairs(trades) do
    local base = "trades[" .. tostring(idx) .. "]"
    if type(trade) ~= "table" then
      validationWarn(base, "item is not an object")
    else
      if tostring(trade.id or "") == "" then
        validationWarn(base .. ".id", "missing required id")
      end
      validateTimestampMsToDate(trade.timestamp, base .. ".timestamp")
      local side = tostring(trade.side or "")
      if side ~= "buy" and side ~= "sell" then
        validationWarn(base .. ".side", "unexpected side (" .. tostring(side) .. ")")
      end
      validateNumericField(trade.amount, base .. ".amount", true)
      validateNumericField(trade.price, base .. ".price", true)
      if string.upper(tostring(trade.feeCurrency or "")) == "EUR" then
        validateNumericField(trade.fee, base .. ".fee", true)
      end
    end
  end
end

local function validateMarketsForDebug(markets)
  if not DEBUG_VALIDATION then
    return
  end
  if type(markets) ~= "table" then
    validationWarn("/v2/markets", "response is not a table")
    return
  end

  for idx, marketInfo in ipairs(markets) do
    local base = "/v2/markets[" .. tostring(idx) .. "]"
    if type(marketInfo) ~= "table" then
      validationWarn(base, "item is not an object")
    else
      if type(marketInfo.market) ~= "string" or marketInfo.market == "" then
        validationWarn(base .. ".market", "missing required market")
      end
    end
  end
end

local function validateTickerForDebug(market, tickerResponse)
  if not DEBUG_VALIDATION then
    return
  end
  validateEurQuoteMarket(market, "ticker.market")
  if type(tickerResponse) ~= "table" then
    validationWarn("ticker", "response is not an object")
    return
  end
  validateNumericField(tickerResponse.price, "ticker.price", true)
end

local function validateAccountCurrencyForDebug(account)
  if not DEBUG_VALIDATION then
    return
  end
  if type(account) ~= "table" then
    validationWarn("account", "account object missing")
    return
  end
  if tostring(account.currency or "") ~= "EUR" then
    validationWarn("account.currency", "expected EUR account currency")
  end
end

-- ---------------------------------------------------------------------------
-- 6) Bitvavo Fetch Helpers (History, Markets, Deposits, Withdrawals, Trades)
-- ---------------------------------------------------------------------------

-- Purpose: Fetch paginated `/v2/account/history` data and flatten all items.
-- Inputs: Optional `maxItems` page size and optional `maxPages` hard page limit.
-- Outputs: `(items, nil)` with concatenated history or `(nil, errText)`.
-- Assumptions: Endpoint returns `{ items, currentPage, totalPages }`.
-- Error behavior: Stops on transport/API/schema errors or safety limit breach.
local function privateGetPagedAccountHistory(maxItems, maxPages)
  local allItems = {}
  local page = 1
  local safeMaxPages = maxPages or 1000
  local strictPageLimit = (maxPages == nil)
  local perPage = maxItems or ACCOUNT_HISTORY_MAX_ITEMS

  while page <= safeMaxPages do
    local path = string.format("/v2/account/history?page=%d&maxItems=%d", page, perPage)
    local res, err = privateGet(path)
    if err ~= nil or res == nil then
      return nil, (err or "unknown")
    end

    if type(res) ~= "table" then
      return nil, "Unexpected response type for /v2/account/history"
    end

    if res.errorCode ~= nil then
      return nil, (res.error or tostring(res.errorCode))
    end

    local items = res.items
    if type(items) ~= "table" then
      return nil, "Missing items in /v2/account/history response"
    end
    validateAccountHistoryItemsForDebug(items, path .. ".items")

    for _, item in ipairs(items) do
      table.insert(allItems, item)
    end

    local totalPages = tonumber(res.totalPages) or 1
    local currentPage = tonumber(res.currentPage) or page
    if currentPage >= totalPages then
      return allItems, nil
    end

    page = page + 1
  end

  if strictPageLimit then
    return nil, "Pagination safety limit exceeded for /v2/account/history"
  end
  return allItems, nil
end

-- ---------------------------------------------------------------------------
-- 7) Cash Transformation (Normalization, Display Strings, Transaction Mapping)
-- ---------------------------------------------------------------------------

local function sortTransactions(transactions)
  table.sort(transactions, function(a, b)
    if a.bookingDate == b.bookingDate then
      return tostring(a.id) < tostring(b.id)
    end
    return a.bookingDate < b.bookingDate
  end)
end

local function makeCashTransaction(bookingDate, amount, titleLine, detailLine, id)
  return {
    bookingDate = bookingDate,
    valueDate = bookingDate,
    amount = amount,
    purpose = detailLine,
    name = titleLine,
    id = id
  }
end

local function urlEncode(value)
  return tostring(value):gsub("([^%w%-_%.~])", function(c)
    return string.format("%%%02X", string.byte(c))
  end)
end

local function baseSymbolFromMarket(market)
  if type(market) ~= "string" then
    return tostring(market or "")
  end
  local base = market:match("^(.-)%-EUR$")
  if base ~= nil and base ~= "" then
    return base
  end
  return market
end

local function formatPriceForDisplay(value)
  local raw = tostring(value or "")
  if raw == "" then
    return raw
  end

  local sign, intPart, fracPart = raw:match("^([%-+]?)(%d+)%.(%d+)$")
  if sign == nil then
    sign, intPart = raw:match("^([%-+]?)(%d+)$")
  end
  if intPart == nil then
    return raw
  end

  local rev = intPart:reverse()
  rev = rev:gsub("(%d%d%d)", "%1.")
  if rev:sub(-1) == "." then
    rev = rev:sub(1, -2)
  end
  local groupedInt = rev:reverse()
  if fracPart ~= nil and fracPart ~= "" then
    return sign .. groupedInt .. "," .. fracPart
  end
  return sign .. groupedInt
end

local function trimTrailingZeros(numberText)
  local trimmed = tostring(numberText or "")
  trimmed = trimmed:gsub("(%..-)0+$", "%1")
  trimmed = trimmed:gsub("%.$", "")
  if trimmed == "" or trimmed == "-0" then
    return "0"
  end
  return trimmed
end

local function formatNumberForDisplay(rawValue, fallbackNumber, fallbackDecimals)
  if rawValue ~= nil then
    local text = tostring(rawValue)
    if text ~= "" then
      return formatPriceForDisplay(text)
    end
  end

  local n = tonumber(fallbackNumber)
  if n == nil then
    return "0"
  end

  local text
  if fallbackDecimals ~= nil then
    text = trimTrailingZeros(string.format("%." .. tostring(fallbackDecimals) .. "f", n))
  else
    text = tostring(n)
    if text:find("[eE]") ~= nil then
      text = trimTrailingZeros(string.format("%.12f", n))
    end
  end

  return formatPriceForDisplay(text)
end

local function formatEurAmount2(value)
  local n = tonumber(value)
  if n == nil then
    n = 0
  end
  return formatPriceForDisplay(string.format("%.2f", n))
end

local function buildCashDisplayLines(normalizedTx)
  local txType = string.upper(tostring(normalizedTx.type or ""))

  if txType == "BUY" or txType == "SELL" then
    local baseAsset = string.upper(tostring(normalizedTx.baseAsset or ""))
    if baseAsset == "" then
      baseAsset = "ASSET"
    end

    local baseAmountNum = toNumberSafe(normalizedTx.baseAmount)
    local quoteAmountNum = toNumberSafe(normalizedTx.quoteAmount)
    local priceNum = toNumberSafe(normalizedTx.price)
    local hasExplicitPrice = normalizedTx.price ~= nil and tostring(normalizedTx.price) ~= "" and priceNum > 0
    if (not hasExplicitPrice) and baseAmountNum > 0 then
      priceNum = quoteAmountNum / baseAmountNum
    end

    local titleLine = (txType == "BUY" and "Kauf " or "Verkauf ") .. baseAsset
    local amountText = formatNumberForDisplay(normalizedTx.baseAmount, baseAmountNum)
    local priceText
    if hasExplicitPrice then
      priceText = formatNumberForDisplay(normalizedTx.price, priceNum)
    else
      priceText = formatNumberForDisplay(nil, priceNum, 8)
    end

    local feeText = formatEurAmount2(normalizedTx.fee)
    local detailLine = amountText .. " " .. baseAsset
      .. " (Kurs: " .. priceText .. " EUR, Gebühr: " .. feeText .. " EUR)"
    return titleLine, detailLine
  end

  if txType == "DEPOSIT" then
    return "Einzahlung", formatEurAmount2(normalizedTx.quoteAmount) .. " EUR"
  end

  if txType == "WITHDRAWAL" then
    return "Auszahlung", formatEurAmount2(normalizedTx.quoteAmount) .. " EUR"
  end

  return "Transaktion", formatEurAmount2(normalizedTx.quoteAmount) .. " EUR"
end

local function normalizeAccountHistoryForDisplay(item)
  local txType = string.upper(tostring(item.type or ""))
  local feeEur = 0
  if string.upper(tostring(item.feesCurrency or "")) == "EUR" then
    feeEur = toNumberSafe(item.feesAmount)
  end

  local normalized = {
    type = txType,
    baseAsset = nil,
    baseAmount = nil,
    quoteAmount = nil,
    price = nil,
    fee = feeEur
  }

  if txType == "BUY" then
    normalized.baseAsset = string.upper(tostring(item.receivedCurrency or ""))
    normalized.baseAmount = item.receivedAmount
    normalized.quoteAmount = item.sentAmount
    if string.upper(tostring(item.priceCurrency or "")) == "EUR" then
      normalized.price = item.priceAmount
    end
    return normalized
  end

  if txType == "SELL" then
    normalized.baseAsset = string.upper(tostring(item.sentCurrency or ""))
    normalized.baseAmount = item.sentAmount
    normalized.quoteAmount = item.receivedAmount
    if string.upper(tostring(item.priceCurrency or "")) == "EUR" then
      normalized.price = item.priceAmount
    end
    return normalized
  end

  if txType == "DEPOSIT" then
    normalized.quoteAmount = item.receivedAmount or item.sentAmount
    return normalized
  end

  if txType == "WITHDRAWAL" then
    normalized.quoteAmount = item.sentAmount or item.receivedAmount
    return normalized
  end

  if string.upper(tostring(item.receivedCurrency or "")) == "EUR" then
    normalized.type = "DEPOSIT"
    normalized.quoteAmount = item.receivedAmount
    return normalized
  end

  if string.upper(tostring(item.sentCurrency or "")) == "EUR" then
    normalized.type = "WITHDRAWAL"
    normalized.quoteAmount = item.sentAmount
    return normalized
  end

  return normalized
end

-- Bitvavo Fetch Helpers (continued: Windowed Lists and Trade/Transfer Endpoints)

-- Purpose: Fetch list endpoints with recursive time-window splitting on truncation.
-- Inputs: `pathBuilder`, `[startMs, endMs]`, recursion `depth`, and `context`.
-- Outputs: `(items, nil)` or `(nil, errText)` if the window cannot be resolved.
-- Assumptions: Endpoint is time-filtered and caps responses at `BACKFILL_LIMIT`.
-- Error behavior: Returns formatted API errors for transport, schema, and limits.
local function fetchWindowedPrivateList(pathBuilder, startMs, endMs, depth, context)
  local path = pathBuilder(startMs, endMs, BACKFILL_LIMIT)
  local res, err = privateGet(path)
  if err ~= nil or res == nil then
    return nil, formatApiError(context, err)
  end
  if type(res) ~= "table" then
    return nil, formatApiError(context, "Unexpected response type")
  end
  if res.errorCode ~= nil then
    return nil, formatApiError(context, res.error or tostring(res.errorCode))
  end

  local count = #res
  if count < BACKFILL_LIMIT then
    return res, nil
  end

  if depth >= MAX_WINDOW_SPLIT_DEPTH then
    return nil, formatApiError(context, "Window split depth exceeded")
  end
  if startMs >= endMs then
    return nil, formatApiError(context, "Cannot split window further")
  end

  local mid = math.floor((startMs + endMs) / 2)
  if mid <= startMs then
    return nil, formatApiError(context, "Window split precision exhausted")
  end

  local left, leftErr = fetchWindowedPrivateList(pathBuilder, startMs, mid, depth + 1, context)
  if leftErr ~= nil or left == nil then
    return nil, leftErr
  end
  -- Split into [start, mid] and [mid+1, end] so recursive windows do not overlap.
  local right, rightErr = fetchWindowedPrivateList(pathBuilder, mid + 1, endMs, depth + 1, context)
  if rightErr ~= nil or right == nil then
    return nil, rightErr
  end

  local combined = {}
  for _, item in ipairs(left) do
    table.insert(combined, item)
  end
  for _, item in ipairs(right) do
    table.insert(combined, item)
  end

  return combined, nil
end

local function fetchDeposits(startMs, endMs)
  local function pathBuilder(windowStartMs, windowEndMs, limit)
    return string.format("/v2/depositHistory?start=%d&end=%d&limit=%d", windowStartMs, windowEndMs, limit)
  end
  return fetchWindowedPrivateList(pathBuilder, startMs, endMs, 0, "depositHistory")
end

local function fetchWithdrawals(startMs, endMs)
  local function pathBuilder(windowStartMs, windowEndMs, limit)
    return string.format("/v2/withdrawalHistory?start=%d&end=%d&limit=%d", windowStartMs, windowEndMs, limit)
  end
  return fetchWindowedPrivateList(pathBuilder, startMs, endMs, 0, "withdrawalHistory")
end

local function fetchAllEurMarkets()
  local res, err = publicGet("/v2/markets")
  if err ~= nil or res == nil then
    return nil, formatApiError("markets", err)
  end
  if type(res) ~= "table" then
    return nil, formatApiError("markets", "Unexpected response type")
  end
  if res.errorCode ~= nil then
    return nil, formatApiError("markets", res.error or tostring(res.errorCode))
  end
  validateMarketsForDebug(res)

  local markets = {}
  for _, item in ipairs(res) do
    if type(item) == "table" and type(item.market) == "string" and item.market:sub(-4) == "-EUR" then
      table.insert(markets, item.market)
    end
  end

  table.sort(markets)
  return markets, nil
end

local function collectCandidateMarketsFromHistory(historyItems, candidates)
  if type(historyItems) ~= "table" then
    return
  end

  for _, item in ipairs(historyItems) do
    if type(item) == "table" then
      local market = string.upper(tostring(item.market or ""))
      if market ~= "" and market:sub(-4) == "-EUR" then
        candidates[market] = true
      end

      local sentCurrency = string.upper(tostring(item.sentCurrency or ""))
      if sentCurrency ~= "" and sentCurrency ~= "EUR" then
        candidates[sentCurrency .. "-EUR"] = true
      end

      local receivedCurrency = string.upper(tostring(item.receivedCurrency or ""))
      if receivedCurrency ~= "" and receivedCurrency ~= "EUR" then
        candidates[receivedCurrency .. "-EUR"] = true
      end
    end
  end
end

local function collectCandidateEurMarkets(balances, historyItems)
  local candidateSet = {}

  for _, b in ipairs(balances or {}) do
    local symbol = string.upper(tostring(b.symbol or ""))
    if symbol ~= "" and symbol ~= "EUR" then
      candidateSet[symbol .. "-EUR"] = true
    end
  end

  collectCandidateMarketsFromHistory(historyItems, candidateSet)

  local candidates = {}
  for market, _ in pairs(candidateSet) do
    table.insert(candidates, market)
  end

  table.sort(candidates)
  return candidates
end

local function selectRelevantEurMarkets(availableMarkets, candidateMarkets)
  local availableSet = {}
  for _, market in ipairs(availableMarkets) do
    availableSet[string.upper(tostring(market or ""))] = true
  end

  local candidateSet = {}
  for _, market in ipairs(candidateMarkets or {}) do
    local key = string.upper(tostring(market or ""))
    if key ~= "" then
      candidateSet[key] = true
    end
  end

  local selectedSet = {}
  local selected = {}
  for _, priorityMarket in ipairs(PRIORITY_BACKFILL_MARKETS) do
    local key = string.upper(tostring(priorityMarket or ""))
    if key ~= "" and candidateSet[key] ~= nil and selectedSet[key] == nil and availableSet[key] ~= nil then
      selectedSet[key] = true
      table.insert(selected, key)
    end
  end

  local remaining = {}
  for key, _ in pairs(candidateSet) do
    if selectedSet[key] == nil and availableSet[key] ~= nil then
      table.insert(remaining, key)
    end
  end
  table.sort(remaining)
  for _, key in ipairs(remaining) do
    selectedSet[key] = true
    table.insert(selected, key)
  end

  if MAX_BACKFILL_MARKETS > 0 and #selected > MAX_BACKFILL_MARKETS then
    local limited = {}
    for i = 1, MAX_BACKFILL_MARKETS do
      table.insert(limited, selected[i])
    end
    selected = limited
  end

  return selected
end

local function fetchTradesForMarket(market, startMs, endMs)
  local encodedMarket = urlEncode(market)
  local context = "trades " .. tostring(market)

  local function pathBuilder(windowStartMs, windowEndMs, limit)
    return string.format(
      "/v2/trades?market=%s&start=%d&end=%d&limit=%d",
      encodedMarket,
      windowStartMs,
      windowEndMs,
      limit
    )
  end

  return fetchWindowedPrivateList(pathBuilder, startMs, endMs, 0, context)
end

-- ---------------------------------------------------------------------------
-- 8) Backfill and Reconciliation Logic
-- ---------------------------------------------------------------------------

-- Purpose: Build full-sync EUR cash transactions from deposits, withdrawals, trades.
-- Inputs: Backfill `[startMs, endMs]`, current `balances`, and history for discovery.
-- Outputs: `(transactions, sumDelta, oldestBookingDate, nil)` or trailing error text.
-- Assumptions: Full sync may need multiple endpoints and market discovery filters.
-- Error behavior: Returns immediately when any required fetch/validation step fails.
local function buildBackfillCashTransactions(startMs, endMs, balances, historyItems)
  local transactions = {}
  -- De-duplicate across all backfill sources and split windows by stable IDs.
  -- IDs are namespace-prefixed for clarity and stability:
  -- - `dep:` timestamp/amount/symbol for deposits
  -- - `wd:` timestamp/amount/symbol/target for withdrawals
  -- - `trade:` market/trade-id for trades
  local seen = {}
  local sumDelta = 0
  local oldestBookingDate = nil

  local function addTransaction(id, bookingDate, amount, titleLine, detailLine)
    if id == nil or id == "" or bookingDate == nil then
      return
    end
    if seen[id] ~= nil then
      return
    end

    seen[id] = true
    table.insert(transactions, makeCashTransaction(bookingDate, amount, titleLine, detailLine, id))
    sumDelta = sumDelta + amount
    if oldestBookingDate == nil or bookingDate < oldestBookingDate then
      oldestBookingDate = bookingDate
    end
  end

  local deposits, depositsErr = fetchDeposits(startMs, endMs)
  if depositsErr ~= nil or deposits == nil then
    return nil, nil, nil, depositsErr
  end
  validateTransferItemsForDebug(deposits, "/v2/depositHistory")

  for _, dep in ipairs(deposits) do
    if type(dep) == "table" and string.upper(tostring(dep.symbol or "")) == "EUR" and dep.status == "completed" then
      local timestampMs = tonumber(dep.timestamp)
      if timestampMs ~= nil then
        local amount = toNumberSafe(dep.amount)
        local fee = toNumberSafe(dep.fee)
        local delta = amount - fee
        if math.abs(delta) > 0 then
          local bookingDate = math.floor(timestampMs / 1000)
          local id = "dep:" .. tostring(timestampMs) .. ":" .. tostring(dep.amount or "") .. ":" .. tostring(dep.symbol or "")
          local titleLine, detailLine = buildCashDisplayLines({
            type = "DEPOSIT",
            quoteAmount = dep.amount
          })
          addTransaction(id, bookingDate, delta, titleLine, detailLine)
        end
      end
    end
  end

  local withdrawals, withdrawalsErr = fetchWithdrawals(startMs, endMs)
  if withdrawalsErr ~= nil or withdrawals == nil then
    return nil, nil, nil, withdrawalsErr
  end
  validateTransferItemsForDebug(withdrawals, "/v2/withdrawalHistory")

  for _, wd in ipairs(withdrawals) do
    if type(wd) == "table" and string.upper(tostring(wd.symbol or "")) == "EUR" and wd.status == "completed" then
      local timestampMs = tonumber(wd.timestamp)
      if timestampMs ~= nil then
        local amount = toNumberSafe(wd.amount)
        local fee = toNumberSafe(wd.fee)
        local delta = -(amount + fee)
        if math.abs(delta) > 0 then
          local bookingDate = math.floor(timestampMs / 1000)
          local target = tostring(wd.txId or wd.address or "")
          local id = "wd:" .. tostring(timestampMs) .. ":" .. tostring(wd.amount or "") .. ":" .. tostring(wd.symbol or "") .. ":" .. target
          local titleLine, detailLine = buildCashDisplayLines({
            type = "WITHDRAWAL",
            quoteAmount = wd.amount
          })
          addTransaction(id, bookingDate, delta, titleLine, detailLine)
        end
      end
    end
  end

  local candidateMarkets = collectCandidateEurMarkets(balances, historyItems)
  if #candidateMarkets == 0 then
    sortTransactions(transactions)
    return transactions, sumDelta, oldestBookingDate, nil
  end

  local eurMarkets, marketsErr = fetchAllEurMarkets()
  if marketsErr ~= nil or eurMarkets == nil then
    return nil, nil, nil, marketsErr
  end

  local selectedMarkets = selectRelevantEurMarkets(eurMarkets, candidateMarkets)
  if #selectedMarkets == 0 then
    sortTransactions(transactions)
    return transactions, sumDelta, oldestBookingDate, nil
  end

  for _, market in ipairs(selectedMarkets) do
    local trades, tradesErr = fetchTradesForMarket(market, startMs, endMs)
    if tradesErr ~= nil or trades == nil then
      return nil, nil, nil, tradesErr
    end
    validateTradesForDebug(market, trades)

    for _, trade in ipairs(trades) do
      if type(trade) == "table" then
        local settled = (trade.settled == true or trade.settled == "true")
        if settled then
          local timestampMs = tonumber(trade.timestamp)
          local side = tostring(trade.side or "")
          local amount = toNumberSafe(trade.amount)
          local price = toNumberSafe(trade.price)
          local tradeId = tostring(trade.id or "")
          if timestampMs ~= nil and tradeId ~= "" and amount > 0 and price > 0 and (side == "buy" or side == "sell") then
            local notional = amount * price
            local feeEur = 0
            if string.upper(tostring(trade.feeCurrency or "")) == "EUR" then
              feeEur = toNumberSafe(trade.fee)
            end

            local delta
            if side == "buy" then
              delta = -(notional + feeEur)
            else
              delta = notional - feeEur
            end

            if math.abs(delta) > 0 then
              local bookingDate = math.floor(timestampMs / 1000)
              local id = "trade:" .. tostring(market) .. ":" .. tradeId
              local baseSymbol = string.upper(baseSymbolFromMarket(market))
              local titleLine, detailLine = buildCashDisplayLines({
                type = string.upper(side),
                baseAsset = baseSymbol,
                baseAmount = trade.amount,
                quoteAmount = notional,
                price = trade.price,
                fee = feeEur
              })
              addTransaction(id, bookingDate, delta, titleLine, detailLine)
            end
          end
        end
      end
    end
  end

  sortTransactions(transactions)
  return transactions, sumDelta, oldestBookingDate, nil
end

-- ID and de-dup rationale:
-- - Prefer Bitvavo `transactionId` when present because it is provider-assigned.
-- - Fallback to a composite key from immutable economic fields when `transactionId`
--   is missing, so repeated reads/pagination still map to the same transaction key.
local function buildTransactionKey(item)
  local txId = item.transactionId
  if txId ~= nil and txId ~= "" then
    return "txid:" .. tostring(txId)
  end

  return table.concat({
    tostring(item.executedAt or ""),
    tostring(item.type or ""),
    tostring(item.sentCurrency or ""),
    tostring(item.sentAmount or ""),
    tostring(item.receivedCurrency or ""),
    tostring(item.receivedAmount or "")
  }, "|")
end

-- Purpose: Transform `/v2/account/history` entries into MoneyMoney cash transactions.
-- Inputs: Raw `items` array and optional incremental boundary `since`.
-- Outputs: `(transactions, sumDelta, oldestBookingDate)` sorted by booking date.
-- Assumptions: Only EUR in/out plus EUR fees affect the EUR cash account delta.
-- Error behavior: Invalid rows are skipped implicitly; no hard failure is raised.
local function buildCashTransactions(items, since)
  local transactions = {}
  -- `seen` prevents duplicates when history pages overlap or repeat across requests.
  local seen = {}
  local sinceNum = tonumber(since)
  local sumDelta = 0
  local oldestBookingDate = nil

  for _, item in ipairs(items) do
    if type(item) == "table" then
      local uniqueKey = buildTransactionKey(item)
      if seen[uniqueKey] == nil then
        seen[uniqueKey] = true

        local executedAtUnix = isoToUnixSeconds(item.executedAt)
        if executedAtUnix ~= nil and (sinceNum == nil or executedAtUnix > sinceNum) then
          local delta = 0
          if string.upper(tostring(item.receivedCurrency or "")) == "EUR" then
            delta = delta + toNumberSafe(item.receivedAmount)
          end
          if string.upper(tostring(item.sentCurrency or "")) == "EUR" then
            delta = delta - toNumberSafe(item.sentAmount)
          end
          if string.upper(tostring(item.feesCurrency or "")) == "EUR" then
            delta = delta - toNumberSafe(item.feesAmount)
          end

          if math.abs(delta) > 0 then
            local normalizedDisplay = normalizeAccountHistoryForDisplay(item)
            local titleLine, detailLine = buildCashDisplayLines(normalizedDisplay)
            local tx = makeCashTransaction(
              executedAtUnix,
              delta,
              titleLine,
              detailLine,
              "bitvavo:" .. uniqueKey
            )

            table.insert(transactions, tx)
            sumDelta = sumDelta + delta
            if oldestBookingDate == nil or executedAtUnix < oldestBookingDate then
              oldestBookingDate = executedAtUnix
            end
          end
        end
      end
    end
  end

  sortTransactions(transactions)

  return transactions, sumDelta, oldestBookingDate
end

-- Purpose: Create the synthetic opening reconciliation cash transaction.
-- Inputs: Reconciliation `amount` and selected `bookingDate`.
-- Outputs: A single MoneyMoney transaction object with stable synthetic ID.
-- Assumptions: Called only in full-sync mode to reconcile opening cash position.
-- Error behavior: No internal error path; caller decides if/when to create it.
local function buildReconciliationTransaction(amount, bookingDate)
  return makeCashTransaction(
    bookingDate,
    amount,
    "Eröffnungsabgleich",
    "Eröffnungsabgleich",
    "bitvavo:opening-reconcile:" .. tostring(bookingDate) .. ":" .. string.format("%.2f", amount)
  )
end

-- Purpose: Resolve one symbol's EUR price via `/v2/ticker/price` with memoization.
-- Inputs: Asset `symbol` and mutable `priceCache` map shared in one refresh.
-- Outputs: Numeric EUR price, `1.0` for EUR, or `nil` when unavailable.
-- Assumptions: Market format is `<SYMBOL>-EUR` and public ticker endpoint is reachable.
-- Error behavior: Returns `nil` on request/validation/parsing failures.
local function getEurPrice(symbol, priceCache)
  if symbol == "EUR" then
    return 1.0
  end

  local market = symbol .. "-EUR"
  if priceCache[market] ~= nil then
    return priceCache[market]
  end

  local res, err = publicGet("/v2/ticker/price?market=" .. market)
  if err ~= nil or res == nil then
    return nil
  end
  validateTickerForDebug(market, res)

  -- Expected dictionary shape: { market = "BTC-EUR", price = "..." }.
  local priceStr = res.price
  if priceStr == nil then
    return nil
  end

  local price = tonumber(priceStr)
  if price == nil then
    return nil
  end

  priceCache[market] = price
  return price
end

-- ---------------------------------------------------------------------------
-- 9) MoneyMoney Entry Points (SupportsBank, InitializeSession, ListAccounts,
--    RefreshAccount, EndSession)
-- ---------------------------------------------------------------------------

function SupportsBank(protocol, bankCode)
  return protocol == ProtocolWebBanking and bankCode == "Bitvavo"
end

function InitializeSession(protocol, bankCode, username, reserved, password)
  apiKey = username
  apiSecret = password
  connection = Connection()

  -- Validate credentials by calling /v2/balance.
  local res, err = privateGet("/v2/balance")
  if err ~= nil or res == nil then
    return LoginFailed
  end
  validateBalancesForDebug(res, "/v2/balance (InitializeSession)")

  -- Bitvavo may also return an error object with `errorCode`.
  if type(res) == "table" and res.errorCode ~= nil then
    return LoginFailed
  end
end

function ListAccounts(knownAccounts)
  local cashAccount = {
    name = "Verrechnungskonto",
    owner = "Bitvavo",
    accountNumber = CASH_ACCOUNT_NUMBER,
    bankCode = "Bitvavo",
    currency = "EUR",
    portfolio = false,
    type = AccountTypeGiro
  }

  local portfolioAccount = {
    name = "Depot",
    owner = "Bitvavo",
    accountNumber = PORTFOLIO_ACCOUNT_NUMBER,
    bankCode = "Bitvavo",
    currency = "EUR",
    portfolio = true,
    type = AccountTypePortfolio
  }

  return { cashAccount, portfolioAccount }
end

-- Purpose: Main MoneyMoney refresh entry point for cash and portfolio accounts.
-- Inputs: Account descriptor `account` and optional incremental timestamp `since`.
-- Outputs: Cash result `{ balance, transactions }` or portfolio `{ securities }`.
-- Assumptions: `/v2/balance` is fetched first; account number selects refresh branch.
-- Error behavior: Returns user-visible API error text when a required fetch fails.
function RefreshAccount(account, since)
  validateAccountCurrencyForDebug(account)

  local effectiveSince = since
  if account.accountNumber == CASH_ACCOUNT_NUMBER and FORCE_FULL_SYNC then
    effectiveSince = nil
  end

  if account.accountNumber == CASH_ACCOUNT_NUMBER then
    if effectiveSince ~= nil then
      resetRequestState(REQUEST_BUDGET_INCREMENTAL)
    else
      resetRequestState(REQUEST_BUDGET_FULLSYNC)
    end
  else
    resetRequestState(REQUEST_BUDGET_PORTFOLIO)
  end

  local balances, err = privateGet("/v2/balance")
  if err ~= nil or balances == nil then
    return "Bitvavo API Fehler: " .. (err or "unknown")
  end

  if type(balances) == "table" and balances.errorCode ~= nil then
    return "Bitvavo API Fehler: " .. (balances.error or balances.errorCode or "unknown")
  end
  validateBalancesForDebug(balances, "/v2/balance (RefreshAccount)")

  -- `eurTotal` is the current EUR balance from `/v2/balance` (`available + inOrder`).
  local eurTotal = 0
  for _, b in ipairs(balances) do
    if string.upper(tostring(b.symbol or "")) == "EUR" then
      local available = toNumberSafe(b.available)
      local inOrder = toNumberSafe(b.inOrder)
      eurTotal = available + inOrder
      break
    end
  end

  -- Cash account path: return current EUR balance plus reconstructed transactions.
  if account.accountNumber == CASH_ACCOUNT_NUMBER then
    local transactions = {}
    -- `sumDelta` is the cumulative EUR delta from the constructed cash transactions.
    local sumDelta = 0
    local oldestBookingDate = nil

    if effectiveSince ~= nil then
      -- Incremental sync path: use `/v2/account/history` and filter by `since`.
      local historyItems, historyErr = privateGetPagedAccountHistory(ACCOUNT_HISTORY_MAX_ITEMS)
      if historyErr ~= nil or historyItems == nil then
        return formatApiError("account/history", historyErr)
      end

      local incrementalTransactions, incrementalSumDelta, incrementalOldestBookingDate =
        buildCashTransactions(historyItems, effectiveSince)
      transactions = incrementalTransactions or {}
      sumDelta = incrementalSumDelta or 0
      oldestBookingDate = incrementalOldestBookingDate
    else
      -- Full sync path: use transfer/trade backfill plus opening reconciliation.
      local historyForMarketDiscovery, discoveryErr = privateGetPagedAccountHistory(
        ACCOUNT_HISTORY_MAX_ITEMS,
        MARKET_DISCOVERY_HISTORY_PAGES
      )
      if discoveryErr ~= nil or historyForMarketDiscovery == nil then
        historyForMarketDiscovery = {}
      end

      local backfillTransactions, backfillSumDelta, backfillOldestBookingDate, backfillErr = buildBackfillCashTransactions(
        0,
        nowMs(),
        balances,
        historyForMarketDiscovery
      )
      if backfillErr ~= nil or backfillTransactions == nil then
        return backfillErr or formatApiError("backfill", "unknown")
      end
      transactions = backfillTransactions
      sumDelta = backfillSumDelta or 0
      oldestBookingDate = backfillOldestBookingDate
    end

    -- Reconciliation is intentionally full-sync only (`since == nil`):
    -- partial/incremental sync has no opening boundary, so no synthetic opener is added.
    if effectiveSince == nil then
      -- Opening adjustment formula:
      -- residual = eurTotal - sumDelta
      -- This records the pre-history opening balance as an opening reconciliation.
      local residual = eurTotal - sumDelta
      if math.abs(residual) >= 0.01 then
        local bookingDate = nowSec()
        if oldestBookingDate ~= nil then
          bookingDate = oldestBookingDate
        end
        table.insert(transactions, buildReconciliationTransaction(residual, bookingDate))
        sortTransactions(transactions)
      end
    end

    return { balance = eurTotal, transactions = transactions }
  end

  -- Portfolio account path: return non-EUR balances as securities.
  local priceCache = {}
  local securities = {}

  for _, b in ipairs(balances) do
    local symbol = string.upper(tostring(b.symbol or ""))
    if symbol ~= "" and symbol ~= "EUR" then
      local available = tonumber(b.available) or 0
      local inOrder = tonumber(b.inOrder) or 0
      local qty = available + inOrder

      if qty > 0 then
        local priceEur = getEurPrice(symbol, priceCache)

        local sec = {
          name = symbol,
          market = "Bitvavo",
          quantity = qty,
          -- For quantity/units, `currencyOfQuantity` must stay nil.
          currencyOfQuantity = nil
        }

        -- Add valuation fields only when an EUR price is available.
        if priceEur ~= nil then
          sec.price = priceEur
          -- Price is in account currency (EUR), so currencyOfPrice can be nil.
          sec.currencyOfPrice = nil
          -- Provide amount in account currency to avoid UI miscalculation.
          sec.amount = qty * priceEur
        end

        table.insert(securities, sec)
      end
    end
  end

  return { securities = securities }
end

function EndSession()
  -- No session teardown required.
end
