# Using the FINOS CDM Python implementation to create a securities trade

This demo generates a valid CDM business event representing a trade by 'bank-a' in a dummy UK Gilt (ISIN: GB00BD0PCK97) denominated in GBP.

All the events are stored in a common directory (eventlogs) and once the trade is completed, the demo summarizes buy and sell total volume, average price and number of trades.

To invoke the demo:
> python trade_demo.py action where action is [B/S][amt]@[price]f[counterparty]'

- B[uy] or S[ell]
- amt of trade. eg 100
- trade price: eg 102
- f[counterparty]: eg fbank-b

for example: to buy 100 from bank-b at 102 
> trade_demo.py B100@102fbank-b

The demo uses the following packages from the FINOS Python CDM implementation: https://central.sonatype.com/artifact/org.finos.cdm/cdm-python/versions
- rosetta_runtime-2.0.0-py3-none-any.whl
- python_cdm-*.*.*-py3-none-any.whl

These rely upon Pydantic 2.5+ which is automatically installed.

Installation of the prettytable package used to format the results is also required.  

The demo was created by FT Advisory, LLC (info@ftadvisory.co) based on the Java demo found in the repo https://github.com/tomhealey-icma/cdm-training-app

It is made available under the [Community Specification License 1.0](./LICENSE.md)  
