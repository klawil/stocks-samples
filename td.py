# Deals with TD Amertirade API

from stocks.models import OAuthRefreshToken, OAuthToken, Option, OptionValue, Job, Variance, TickerValue, Strangle, Position, PositionValue
from django.utils import timezone
from django.conf import settings
from django.db.models import F
from stocks.utils.options import buildExpectedValueOfConstruct, buildExpectedValueOfPosition

from datetime import timedelta, datetime

import requests
import json

def millisecondsToDate(milli, date=False):
  if date:
    return datetime.fromtimestamp(milli / 1000).date()
  return timezone.make_aware(datetime.fromtimestamp(milli / 1000))

# Get the current authorization token. Automatically refreshes the token if
# needed
def getToken():
  # Get the most recent token
  try:
    token = OAuthToken.objects.latest('expires')
  except:
    token = OAuthToken(
      expires=timezone.now() - timedelta(days=1)
    )

  # See if the token is expired
  if token.expires < timezone.now() + timedelta(minutes=1):
    token = refreshToken(token)

  return token.token

def refreshToken(token):
  # Get the refresh token
  refreshToken = OAuthRefreshToken.objects.latest('expires')

  # Make the API request to get the new token
  postData = {
    'grant_type': 'refresh_token',
    'refresh_token': refreshToken.token,
    'access_type': 'offline',
    'client_id': settings.TD_CLIENT_ID,
    'redirect_uri': settings.TD_REDIRECT_URI
  }

  # Make the request
  req = requests.post(url="https://api.tdameritrade.com/v1/oauth2/token", data=postData)

  # Parse the response
  response = json.loads(req.text)

  # Save the new access token
  token.token = response['access_token']
  token.expires = timezone.now() + timedelta(seconds=response['expires_in'])
  token.save()

  # Save the new refresh token
  refreshToken.token = response['refresh_token']
  refreshToken.expires = timezone.now() + timedelta(seconds=response['refresh_token_expires_in'])
  refreshToken.save()

  return token

def importOptions(args):
  # Build the arguments
  ticker = args['ticker']
  job = args['job']
  logger = args['logger']
  startDate = timezone.now()
  endDate = timezone.now() + timedelta(days=70)

  logger.debug(f"[{ticker.ticker}] Start Options Import")

  try:
    # Build the URL
    url = f"https://api.tdameritrade.com/v1/marketdata/chains?symbol={ticker.ticker}&fromDate={startDate.strftime('%Y-%m-%d')}&toDate={endDate.strftime('%Y-%m-%d')}&includeQuotes=TRUE"

    # Make the headers
    headers = {
      'Authorization': f"Bearer {getToken()}"
    }

    # Make the request
    req = requests.get(url=url, headers=headers)

    # Parse the response
    response = json.loads(req.text)
    try:
      if not response['status'] == 'SUCCESS':
        job.errors += 1
        job.completed += 1
        job.save()
        logger.error(f"[{ticker.ticker}] API call {response['status']}")
        return
    except:
      job.errors += 1
      job.completed += 1
      job.save()
      logger.error(f"[{ticker.ticker}] API Error: {json.dumps(response)}")
      return

    # Add a tickerValue
    stock = response['underlying']
    tv = TickerValue(
      ticker=ticker,
      job=job,
      quoteTime=millisecondsToDate(stock['quoteTime']),
      tradeTime=millisecondsToDate(stock['tradeTime']),
      mark=stock['mark'],
      last=stock['last'],
      bid=stock['bid'],
      bidSize=stock['bidSize'],
      ask=stock['ask'],
      askSize=stock['askSize'],
      volume=stock['totalVolume']
    )
    tv.save()

    # Import the options
    newOptions = []
    optionValues = {
      'call': parseOptionDateObject(response['callExpDateMap'], ticker, job, newOptions),
      'put': parseOptionDateObject(response['putExpDateMap'], ticker, job, newOptions)
    }
    logger.debug(f"[{ticker.ticker}] {len(newOptions)} options found")
    if len(newOptions) > 0:
      Option.objects.bulk_create(newOptions, ignore_conflicts=True)

    # Log that we are valuing the options now
    logger.debug(f"[{ticker.ticker}] Valuing Options")

    # Merge the options into a new array
    optionValuesByDate = {}
    for callValue in optionValues['call']:
      dateStr = callValue.option.expires.strftime('%Y-%m-%d')

      if dateStr not in optionValuesByDate:
        optionValuesByDate[dateStr] = []

      optionValuesByDate[dateStr].append(callValue)
    for putValue in optionValues['put']:
      dateStr = putValue.option.expires.strftime('%Y-%m-%d')

      if dateStr not in optionValuesByDate:
        optionValuesByDate[dateStr] = []

      optionValuesByDate[dateStr].append(putValue)

    # Calculate the option and strangle values
    strangles = []
    optionValueInserts = []
    for key in optionValuesByDate:
      valueOptions(optionValuesByDate[key], ticker, tv, job, logger, strangles, optionValueInserts)

    # Create the OptionValues
    logger.debug(f"[{ticker.ticker}] {len(optionValueInserts)} option values found")
    if len(optionValueInserts) > 0:
      OptionValue.objects.bulk_create(optionValueInserts)

    # Create the strangles
    logger.debug(f"[{ticker.ticker}] {len(strangles)} strangles found")
    if len(strangles) > 0:
      Strangle.objects.bulk_create(strangles)

    # Value any positions associated with this ticker
    positionValues = []
    for position in Position.objects.filter(ticker=ticker, active=True).select_related().all():
      valuePosition(position, tv, job, positionValues, logger)
    if len(positionValues) > 0:
      PositionValue.objects.bulk_create(positionValues)

    logger.debug(f"[{ticker.ticker}] Complete")
    job.completed += 1
    job.save()
    return json.dumps(response)
  except Exception as e:
    job.errors += 1
    job.completed += 1
    job.save()
    logger.error(f"[{ticker.ticker}] Error: {e}")

def parseOptionDateObject(data, ticker, job, newOptions):
  optionValues = []

  for key in data:
    dateData = data[key]

    for key2 in dateData:
      # Pull out the option data
      option = dateData[key2][0]

      # Get the option value
      o = Option(
        symbol=option['symbol'],
        ticker=ticker,
        optionType=option['putCall'],
        strike=option['strikePrice'],
        expires=millisecondsToDate(option['expirationDate'], date=True)
      )
      newOptions.append(o)

      # Build the option value
      oValue = OptionValue(
        key=f"{o.symbol}-{job.timeRun.strftime('%Y%m%d%H%M%S')}",
        option=o,
        time=millisecondsToDate(option['quoteTimeInLong']),
        job=job,
        mark=option['mark'],
        bid=option['bid'],
        bidSize=option['bidSize'],
        ask=option['ask'],
        askSize=option['askSize'],
        last=option['last'],
        lastSize=option['lastSize'],
        volume=option['totalVolume']
      )
      optionValues.append(oValue)

  return optionValues

def getSamplesForDate(start, end, ticker):
  # Get the number of days until expiration
  daysUntilExpiration = (end - start).days - 1

  # Make sure we aren't starting on a weekend
  if daysUntilExpiration % 7 in [ 5, 6 ]:
    daysUntilExpiration = daysUntilExpiration - (daysUntilExpiration % 7) + 4

  # Make sure the number isn't negative
  if daysUntilExpiration < 0:
    return {
      'success': False,
      'message': f"[{ticker.ticker}] Negative daysUntilExpiration ({daysUntilExpiration}) for {end.strftime('%Y-%m-%d')}"
    }

  # Query for samples
  samples = ticker.variance_set.filter(
    days=daysUntilExpiration,
    closePrice__isnull=False,
    openPrice__gt=0
  )

  # Make sure we have enough samples
  if samples.count() < 100:
    return {
      'success': False,
      'message': f"[{ticker.ticker}] Not enough samples ({samples.count()}) for {daysUntilExpiration} day(s) until expiration"
    }

  # Turn everything into percentages
  changes = []
  for sample in samples:
    changes.append({
      'close': float((sample.closePrice - sample.openPrice) / sample.openPrice),
      'min': float((sample.minPrice - sample.openPrice) / sample.openPrice),
      'max': float((sample.maxPrice - sample.openPrice) / sample.openPrice),
    })

  return {
    'success': True,
    'changes': changes
  }

def valueOptions(optionValues, ticker, tickerValue, importJob, logger, strangles, optionValueInserts):
  changes = getSamplesForDate(importJob.timeRun.date(), optionValues[0].option.expires, ticker)

  # Make sure daysUntilExpiration isn't negative
  if not changes['success']:
    logger.debug(changes['message'])
    OptionValue.objects.bulk_create(optionValues)
    return
  changes = changes['changes']

  # Value the options
  outOfMoney = {
    'PUT': [],
    'CALL': []
  }
  for optionValue in optionValues:
    # Pull out the option information
    strike = optionValue.option.strike
    optionType = optionValue.option.optionType

    # Build the expected value
    valueData = buildExpectedValueOfConstruct({
      'type': optionType,
      'strike': strike,
      'price': optionValue.mark
    }, tickerValue.mark, changes)

    # Save the expected value and profitability
    optionValue.profitable = valueData['profitable']
    optionValue.swing_profitable = valueData['swing_profitable']
    optionValue.swing_profitable5 = valueData['swing_profitable5']
    optionValue.swing_profitable10 = valueData['swing_profitable10']
    optionValue.swing_profitable15 = valueData['swing_profitable15']
    optionValue.swing_profitable20 = valueData['swing_profitable20']
    optionValue.swing_profitable25 = valueData['swing_profitable25']
    optionValue.swing_profitable50 = valueData['swing_profitable50']
    optionValue.swing_profitable75 = valueData['swing_profitable75']
    optionValue.swing_profitable100 = valueData['swing_profitable100']
    optionValueInserts.append(optionValue)

  for optionValue in optionValues:
    # Pull out the option information
    strike = optionValue.option.strike
    optionType = optionValue.option.optionType

    # Add to the list of in the money items (if needed)
    if optionType == 'PUT' and strike < tickerValue.mark:
      outOfMoney[optionType].append(optionValue)
    if optionType == 'CALL' and strike > tickerValue.mark:
      outOfMoney[optionType].append(optionValue)

  # Sort and limit the items
  outOfMoney['PUT'].sort(key=lambda o: o.option.strike, reverse=True)
  outOfMoney['CALL'].sort(key=lambda o: o.option.strike)
  outOfMoney['PUT'] = outOfMoney['PUT'][:5]
  outOfMoney['CALL'] = outOfMoney['CALL'][:5]

  # Build and value the strangles
  for putValue in outOfMoney['PUT']:
    for callValue in outOfMoney['CALL']:
      # Build the strangle
      strangle = Strangle(
        ticker=ticker,
        put=putValue,
        call=callValue,
        expires=callValue.option.expires,
        job=importJob
      )

      # Add the expected value and profitability
      valueData = buildExpectedValueOfConstruct({
        'type': 'STRANGLE',
        'price': callValue.mark + putValue.mark,
        'call': callValue.option.strike,
        'put': putValue.option.strike
      }, tickerValue.mark, changes)
      strangle.profitable = valueData['profitable']
      strangle.swing_profitable = valueData['swing_profitable']
      strangle.swing_profitable5 = valueData['swing_profitable5']
      strangle.swing_profitable10 = valueData['swing_profitable10']
      strangle.swing_profitable15 = valueData['swing_profitable15']
      strangle.swing_profitable20 = valueData['swing_profitable20']
      strangle.swing_profitable25 = valueData['swing_profitable25']
      strangle.swing_profitable50 = valueData['swing_profitable50']
      strangle.swing_profitable75 = valueData['swing_profitable75']
      strangle.swing_profitable100 = valueData['swing_profitable100']
      strangles.append(strangle)

def valuePosition(position, tickerValue, importJob, positionValues, logger):
  logger.debug(f"[{tickerValue.ticker.ticker}] Valuing position {position.name}")

  # Get the samples for the expiration dates
  changeData = getSamplesForDate(importJob.timeRun.date(), position.options.first().expires, tickerValue.ticker)
  if not changeData['success']:
    logger.debug(changeData['message'])
    return

  # Build a valuation of the position
  valueData = buildExpectedValueOfPosition(position, tickerValue.mark, changeData['changes'])
  pv = PositionValue(
    position=position,
    job=importJob,
    profitable=valueData['profitable'],
    swing_profitable=valueData['swing_profitable'],
    swing_profitable5=valueData['swing_profitable5'],
    swing_profitable10=valueData['swing_profitable10'],
    swing_profitable15=valueData['swing_profitable15'],
    swing_profitable20=valueData['swing_profitable20'],
    swing_profitable25=valueData['swing_profitable25'],
    swing_profitable50=valueData['swing_profitable50'],
    swing_profitable75=valueData['swing_profitable75'],
    swing_profitable100=valueData['swing_profitable100']
  )
  positionValues.append(pv)

def importTickerVariance(params):
  try:
    # Pull out the information
    ticker = params['ticker']
    logger = params['logger']
    job = params['job']

    # Log the start
    logger.debug(f"[{ticker.ticker}] Importing variance")

    # Get the last imported date
    lastImport = ticker.variance_set \
      .filter(closePrice__isnull=False) \
      .order_by('-endDate') \
      .first()

    # Figure out the start date
    startDate = timezone.now() - timedelta(days=7)
    if lastImport is None:
      startDate = timezone.now() - timedelta(days=5475)
    elif lastImport.endDate > startDate.date():
      logger.debug(f"[{ticker.ticker}] No import needed")
      job.completed += 1
      job.save()
      return

    # Make the API request
    url = f"https://api.tdameritrade.com/v1/marketdata/{ticker.ticker}/pricehistory?apikey={settings.TD_CLIENT_ID}&periodType=month&frequencyType=daily&endDate={round(timezone.now().timestamp()) * 1000}&startDate={round(startDate.timestamp()) * 1000}"
    req = requests.get(url=url)
    resp = json.loads(req.text)

    # Insert the days (if needed)
    varianceInserts = []
    varianceUpdates = []
    varianceMinMaxUpdates = []
    for day in resp['candles']:
      # Parse the date
      startDate = millisecondsToDate(day['datetime'], date=True)

      # Loop over 53 weeks and insert all the fridays
      weeks = 0
      endDate = startDate + timedelta(days=(4 - startDate.weekday()))
      while weeks <= 10:
        v = Variance(
          ticker=ticker,
          startDate=startDate,
          endDate=endDate,
          days=(endDate - startDate).days,
          openPrice=day['open'],
          minPrice=day['low'],
          maxPrice=day['high']
        )
        varianceInserts.append(v)

        weeks += 1
        endDate = endDate + timedelta(days=7)

      # Update all of the end dates for this date
      if startDate.weekday() == 4:
        varianceUpdates.append({
          'endDate': startDate,
          'close': day['close']
        })

      # Update the mins and maxes
      varianceMinMaxUpdates.append({
        'date': startDate,
        'min': day['low'],
        'max': day['high']
      })

    # Insert the new variances
    Variance.objects.bulk_create(varianceInserts, ignore_conflicts=True)

    # Update the existing variances
    for update in varianceUpdates:
      Variance.objects \
        .filter(
          ticker__exact=ticker,
          endDate__exact=update['endDate']
        ) \
        .update(closePrice=update['close'])

    # Update the mins and maxes
    for update in varianceMinMaxUpdates:
      Variance.objects \
        .filter(
          ticker__exact=ticker,
          endDate__gte=update['date'],
          startDate__lte=update['date'],
          minPrice__gt=update['min']
        ) \
        .update(minPrice=update['min'])
      Variance.objects \
        .filter(
          ticker__exact=ticker,
          endDate__gte=update['date'],
          startDate__lte=update['date'],
          maxPrice__lt=update['max']
        ) \
        .update(maxPrice=update['max'])

    logger.debug(f"[{ticker.ticker}] Done importing variance")
    job.completed += 1
    job.save()
    return json.dumps(resp)
  except Exception as e:
    logger.error(f"[{ticker.ticker}] Error: {e}")
    job.errors += 1
    job.completed += 1
    job.save()
