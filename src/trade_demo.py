'''demo of creating a securities trade using the python implementation of CDM
created by FT Advisory info@ftadvisory.co
v1.0, 15-May-2024
requires the PrettyTable package to format the summary
License: Community Specification License 1.0'''
import uuid
from pathlib import Path
from decimal import Decimal
import datetime
import zoneinfo
from glob import glob
import os
import sys
from prettytable import PrettyTable
from pydantic import ValidationError
from cdm.base.staticdata.party.Party import Party
from cdm.event.workflow.EventTimestamp import EventTimestamp
from cdm.event.workflow.EventTimestampQualificationEnum import EventTimestampQualificationEnum
from cdm.base.staticdata.identifier.Identifier import Identifier
from cdm.base.staticdata.party.PartyRole import PartyRole
from cdm.base.staticdata.party.PartyRoleEnum import PartyRoleEnum
from cdm.event.common.TradeIdentifier import TradeIdentifier
from cdm.base.staticdata.identifier.AssignedIdentifier import AssignedIdentifier
from cdm.base.staticdata.identifier.TradeIdentifierTypeEnum import TradeIdentifierTypeEnum
from cdm.event.common.ExecutionDetails import ExecutionDetails
from cdm.base.staticdata.party.LegalEntity import LegalEntity
from cdm.event.common.ExecutionTypeEnum import ExecutionTypeEnum
from cdm.product.template.Product import Product
from cdm.base.staticdata.asset.common.Security import Security
from cdm.base.staticdata.asset.common.ProductIdentifier import ProductIdentifier
from cdm.base.staticdata.asset.common.ProductIdTypeEnum import ProductIdTypeEnum
from cdm.product.common.settlement.PriceQuantity import PriceQuantity
from cdm.observable.asset.Price import Price
from cdm.product.common.settlement.SettlementTerms import SettlementTerms
from cdm.product.common.settlement.PhysicalSettlementTerms import PhysicalSettlementTerms
from cdm.product.common.settlement.PhysicalSettlementPeriod import PhysicalSettlementPeriod
from cdm.product.common.settlement.SettlementTypeEnum import SettlementTypeEnum
from cdm.base.math.UnitType import UnitType
from cdm.observable.asset.PriceTypeEnum import PriceTypeEnum
from cdm.observable.asset.PriceExpressionEnum import PriceExpressionEnum
from cdm.base.math.NonNegativeQuantitySchedule import NonNegativeQuantitySchedule
from cdm.base.staticdata.party.Counterparty import Counterparty
from cdm.base.staticdata.party.CounterpartyRoleEnum import CounterpartyRoleEnum
from cdm.event.common.ExecutionInstruction import ExecutionInstruction
from cdm.event.common.Instruction import Instruction
from cdm.event.common.PrimitiveInstruction import PrimitiveInstruction
from cdm.event.common.BusinessEvent import BusinessEvent
from cdm.event.workflow.Workflow import  Workflow
from cdm.base.staticdata.asset.common.SecurityTypeEnum import SecurityTypeEnum
from cdm.base.staticdata.party.PartyIdentifier import PartyIdentifier
from cdm.event.common.TradeState import TradeState
from cdm.event.common.Trade import Trade
from cdm.product.template.TradeLot import TradeLot
from cdm.product.template.TradableProduct import TradableProduct
from cdm.event.common.State import State
from cdm.event.position.PositionStatusEnum import PositionStatusEnum

from rosetta.runtime.utils import ConditionViolationError

DEBUG = False

def create_event_timestamp () -> EventTimestamp:
    '''create a timestamp using current time'''
    event_timestamp = EventTimestamp(dateTime=datetime.datetime.now (),
                                     qualification=EventTimestampQualificationEnum.EVENT_CREATION_DATE_TIME)
    validate_pydantic_object(event_timestamp)
    return event_timestamp

def create_trade_business_event(action: str,
                                security_isin: str,
                                security_issuer: str,
                                security_ccy: str,
                                quantity: str,
                                price: str,
                                party1: str,
                                party2: str,
                                trade_uti: str,
                                execution_venue_mic: str,
                                execution_type_mic: str,
                                execution_type: str,
                                trade_date_utc_iso: str,
                                local_tz: str) -> BusinessEvent:
    '''create a business event to match the trade'''
    action_lower = action.lower()
    buyer = party1 if action_lower == "buy" else party2
    seller = party2 if action_lower == "buy" else party1
    parties = [Party(name=buyer, partyId=[PartyIdentifier(identifier=buyer)]),
               Party(name=seller, partyId=[PartyIdentifier(identifier=seller)])]
    validate_pydantic_object(parties[0])
    party_roles = [PartyRole(partyReference=parties[0], role=PartyRoleEnum.BUYER ),
                   PartyRole(partyReference=parties[1], role=PartyRoleEnum.SELLER)]
    validate_pydantic_object(party_roles[0])
    trade_date_local = datetime.datetime.fromisoformat(trade_date_utc_iso).astimezone(zoneinfo.ZoneInfo(local_tz))
    trade_date = datetime.datetime(year=trade_date_local.year,
                                   month=trade_date_local.month,
                                   day=trade_date_local.day)
    trade_id = TradeIdentifier(assignedIdentifier=[AssignedIdentifier(identifier=trade_uti)],
                               issuer=security_issuer,
                               identifierType=TradeIdentifierTypeEnum.UNIQUE_TRANSACTION_IDENTIFIER)
    validate_pydantic_object(trade_id)
    execution_details = ExecutionDetails(executionType=ExecutionTypeEnum[execution_type],
                                         executionVenue=LegalEntity(entityId=[execution_type_mic],
                                                                    name=execution_venue_mic))
    validate_pydantic_object(execution_details)
    product_id = ProductIdentifier(source=ProductIdTypeEnum.ISIN, identifier=security_isin)
    validate_pydantic_object(product_id)
    product = Product(security=Security(productIdentifier=[product_id],
                                        productTaxonomy=[],
                                        securityType=SecurityTypeEnum.DEBT))
    validate_pydantic_object(product)
    price = Price(value=Decimal(price),
                  unit=UnitType(currency=security_ccy),
                  perUnitOf=UnitType(currency=security_ccy),
                  priceType=PriceTypeEnum.ASSET_PRICE,
                  priceExpression=PriceExpressionEnum.PERCENTAGE_OF_NOTIONAL)
    validate_pydantic_object(price)
    quantity = NonNegativeQuantitySchedule(value=Decimal(quantity),
                                           unit=UnitType(currency=security_ccy))
    validate_pydantic_object(quantity)
    settlement_terms = SettlementTerms(settlementType=SettlementTypeEnum.PHYSICAL,
                                       physicalSettlementTerms=PhysicalSettlementTerms(physicalSettlementPeriod=PhysicalSettlementPeriod(businessDays=1)))
    validate_pydantic_object(settlement_terms)
    price_quantity = PriceQuantity(price=[price],
                                   quantity=[quantity],
                                   settlementTerms=settlement_terms)
    validate_pydantic_object(price_quantity)
    counterparties = [Counterparty(role=CounterpartyRoleEnum.PARTY_1,partyReference=parties[0]),
                      Counterparty(role=CounterpartyRoleEnum.PARTY_2,partyReference=parties[1])]
    validate_pydantic_object(counterparties[0])
    execution_instruction = ExecutionInstruction(product=product,
                                                 priceQuantity=[price_quantity],
                                                 counterparty=counterparties,
                                                 parties=parties,
                                                 partyRoles=party_roles,
                                                 executionDetails=execution_details,
                                                 tradeDate=trade_date,
                                                 tradeIdentifier=[trade_id])
    validate_pydantic_object(execution_instruction)
    current_date = datetime.datetime.now()
    event_date = datetime.datetime(current_date.year, current_date.month, current_date.day)
    primitive_instruction = PrimitiveInstruction(execution=execution_instruction)
    validate_pydantic_object(primitive_instruction)
    instruction = Instruction(primitiveInstruction=primitive_instruction, before=None)
    validate_pydantic_object(instruction)
    event = BusinessEvent(instruction=[instruction], eventDate=event_date, effectiveDate=event_date)
    event.after = [create_trade_state_from_event (event)]
    return event if validate_pydantic_object(event) else None
def extract_info_from_event (event: BusinessEvent) -> dict:
    '''extract info from an event'''
    if len(event.instruction) == 0:
        return None
    results = {}
    execution_instruction = event.instruction[0].primitiveInstruction.execution
    results['trade_date'] = execution_instruction.tradeDate
    results['trade_id'] = Identifier(issuer=execution_instruction.tradeIdentifier[0].issuer,
                                     assignedIdentifier=execution_instruction.tradeIdentifier[0].assignedIdentifier)
    results['priceQuantity'] = execution_instruction.priceQuantity
    results['product'] = execution_instruction.product
    results['counterparty'] = execution_instruction.counterparty
    results['tradeIdentifier'] = execution_instruction.tradeIdentifier
    results['parties'] = execution_instruction.parties
    results['partyRoles'] = execution_instruction.partyRoles,
    results['executionDetails'] = execution_instruction.executionDetails
    return results
def create_trade_state_from_event (event: BusinessEvent) -> TradeState:
    '''create a trade state from an event'''
    if len(event.instruction) > 0:
        execution_instruction = event.instruction[0].primitiveInstruction.execution
        trade_id = Identifier(issuer=execution_instruction.tradeIdentifier[0].issuer,
                              assignedIdentifier=execution_instruction.tradeIdentifier[0].assignedIdentifier)
        validate_pydantic_object(trade_id)
        trade_lot = TradeLot(lotIdentifier=[trade_id],
                             priceQuantity=execution_instruction.priceQuantity)
        validate_pydantic_object(trade_lot)
        tradable_product = TradableProduct (product=execution_instruction.product,
                                            tradeLot=[trade_lot],
                                            counterparty=execution_instruction.counterparty)
        validate_pydantic_object(tradable_product)
        trade = Trade(tradeIdentifier=execution_instruction.tradeIdentifier,
                      tradeDate=execution_instruction.tradeDate,
                      tradableProduct=tradable_product,
                      party=execution_instruction.parties,
                      partyRole=execution_instruction.partyRoles,
                      executionDetails=execution_instruction.executionDetails)
        validate_pydantic_object(trade)
        trade_state = TradeState (trade=trade,
                                  state=State(positionState=PositionStatusEnum.EXECUTED))
        return trade_state if validate_pydantic_object(trade_state) else None
    return None

def create_log_file_path (data_dir: Path, data_file_name_in: str) -> Path:
    '''create unique log file path by appending a version to data_file_name'''
    version = 0
    data_file_name = data_file_name_in.replace(' ', '-').replace(':','-').replace('.', '-')
    data_file = data_dir / (data_file_name + "_" + str(version)+ ".json")
    while data_file.exists():
        version += 1
        data_file = data_dir / (data_file_name + "_" + str(version)+ ".json")
    return data_file

def log (data_file_name_in: str, data: str) -> str:
    '''log events'''
    eventlog_dir = Path.cwd().absolute() / 'eventlogs'
    eventlog_dir.mkdir(exist_ok=True)
    log_file_path = create_log_file_path(data_dir=eventlog_dir, data_file_name_in=data_file_name_in)
    with open(log_file_path, 'w') as f:
        f.write(data)
    return log_file_path.name

def log_event (e: BusinessEvent) -> str:
    '''log events'''
    if e is not None and len(e.instruction) > 0 and len(e.instruction[0].primitiveInstruction.execution.tradeIdentifier) > 0:
        return log ('event_' + e.instruction[0].primitiveInstruction.execution.tradeIdentifier[0].assignedIdentifier[0].identifier,
                    e.model_dump_json(indent=2, exclude_defaults=True))
    return None

def log_workflow (w: Workflow) -> str:
    '''log workflow'''
    if w is not None and len(w.steps) > 0 and len(w.steps[0].eventIdentifier) > 0:
        return log ('workflow_' + w.steps[0].eventIdentifier[0].assignedIdentifier[0].identifier,
                    w.model_dump_json(indent=2, exclude_defaults=True))
    return None

def read_event (event_file_name: str) -> BusinessEvent:
    '''create an event from a JSON file'''
    event_path = Path.cwd().absolute() / 'eventlogs' / event_file_name
    with open(event_path, 'r') as f:
        event_json = f.read()
    event = BusinessEvent.model_validate_json (event_json)
    validate_pydantic_object (event)
    return event

def validate_pydantic_object (obj) -> bool:
    '''validate pydantic objects'''
    try:
        obj.validate_model()
        if (DEBUG):
            print(f"validating type {type(obj)}...successful")
        return True
    except (ConditionViolationError, ValidationError) as e:
        print(f"validating type {type(obj)}...failed")
        print(e)
        return False

def parse_action (action_str: str) -> dict:
    '''parse command line into inputs for trade creation'''
    results = {}
    results['action'] = 'Buy' if action_str[0] == 'B' else 'Sell' if action_str[0] == 'S' else None
    if results['action'] is None:
        return None
    quantity = action_str.find('@')
    if quantity < 1:
        return None
    results['quantity'] = action_str[1:quantity]
    side = action_str.find('f')
    if side < quantity:
        return None
    results['price'] = action_str[quantity+1:side]
    results['party2'] = action_str[side+1:len(action_str)]
    return results

def main (action_parse_in: dict):
    '''main'''
    action = action_parse_in['action']
    security_isin = 'GB00BD0PCK97'
    security_ccy = 'GBP'
    security_issuer = 'Gilt'
    quantity = action_parse_in['quantity']
    price = action_parse_in['price']
    party1 = 'bank-a'
    party2 = action_parse_in['party2']
    execution_venue_mic = 'bots'
    execution_venue_type_mic = 'otc'
    execution_venue_type = 'OFF_FACILITY'
    trade_uti = 'CDM' + str(uuid.uuid4())
    trade_date = str(datetime.datetime.now(tz=datetime.UTC))

    event = create_trade_business_event(action = action,
                                        security_isin=security_isin,
                                        security_ccy=security_ccy,
                                        security_issuer=security_issuer,
                                        quantity=quantity,
                                        price=price,
                                        party1=party1,
                                        party2=party2,
                                        execution_venue_mic=execution_venue_mic,
                                        execution_type_mic=execution_venue_type_mic,
                                        execution_type=execution_venue_type,
                                        trade_uti=trade_uti,
                                        trade_date_utc_iso=trade_date,
                                        local_tz='US/Eastern')
    if event is None:
        print('unable to create event')
    else:
        event_file = log_event(event)
        print(f'...created event on {event.eventDate} and saved to {event_file}')
        event_copy = read_event (event_file)
        print(f'...read event from {event_file} it was created on {event_copy.eventDate}')

def get_all_events () -> list[BusinessEvent]:
    '''all the events in the eventlogs directory'''
    event_path = Path.cwd().absolute() / 'eventlogs/'
    event_files = glob(os.path.join(event_path, "event*.json"))
    event_list = []
    for event_file in event_files:
        with open(event_path / event_file, 'r') as f:
            event_json = f.read()
        event = BusinessEvent.model_validate_json (event_json)
        if validate_pydantic_object (event):
            event_list.append (event)
    return event_list

def process_aggregation (side: str, trade: dict, aggregation: dict)-> dict:
    '''process aggregation'''
    if side not in ['buy', 'sell']:
        return None
    aggregation[side]['no_trades'] += 1
    aggregation[side]['volume'] += trade['quantity']
    aggregation[side]['avg_px'] += (trade['quantity'] * trade['price'])
    aggregation['trades'].append ({'trade_date': trade['trade_date'],
                                   'quantity': trade['quantity'],
                                   'price': trade['price']})
    return aggregation

def aggregate_events (event_list: list[BusinessEvent]):
    '''aggregate events'''
    aggregation = {}
    for event in event_list:
        event_info = extract_info_from_event (event)
        buyer_idx, seller_idx = (0,1) if event_info['partyRoles'] == PartyRoleEnum.BUYER else (1,0)
        trade = {'quantity': event_info['priceQuantity'][0].quantity[0].value,
                 'price': event_info['priceQuantity'][0].price[0].value,
                 'trade_date': event_info['trade_date']}
        if event_info['parties'][buyer_idx].name not in aggregation:
            aggregation[event_info['parties'][buyer_idx].name] = {
                'buy': {
                    'volume': 0, 
                    'avg_px': 0, 
                    'no_trades': 0},
                'sell': {
                    'volume': 0, 
                    'avg_px': 0, 
                    'no_trades': 0},
                'trades': []}
        aggregation[event_info['parties'][buyer_idx].name] = process_aggregation (side='buy',
                                                                                  trade=trade,
                                                                                  aggregation=aggregation[event_info['parties'][buyer_idx].name])
        if event_info['parties'][seller_idx].name not in aggregation:
            aggregation[event_info['parties'][seller_idx].name] = {
                'buy': {
                    'volume': 0, 
                    'avg_px': 0, 
                    'no_trades': 0},
                'sell': {
                    'volume': 0, 
                    'avg_px': 0, 
                    'no_trades': 0},
                'trades': []}
        aggregation[event_info['parties'][seller_idx].name] = process_aggregation (side='sell',
                                                                                   trade=trade,
                                                                                   aggregation=aggregation[event_info['parties'][seller_idx].name])
    for value in aggregation.values():
        if value['buy']['no_trades'] > 0:
            value['buy']['avg_px'] = value['buy']['avg_px'] / value['buy']['volume']
        if value['sell']['no_trades'] > 0:
            value['sell']['avg_px'] = value['sell']['avg_px'] / value['sell']['volume']
    return aggregation

def print_events (eventslist: list[BusinessEvent]):
    '''print events starting with bank-a data'''
    aggregation = aggregate_events (eventslist)
    table = PrettyTable(['name', 'side', 'volume', 'avg price', 'no trades'])
    k = 'bank-a'
    v = aggregation[k]
    table.add_row([k,
                    'buy',
                    f"{v['buy']['volume']:.4f}",
                    f"{v['buy']['avg_px']:.4f}",
                    str(v['buy']['no_trades'])])
    table.add_row(['',
                    'sell',
                    f"{v['sell']['volume']:.4f}",
                    f"{v['sell']['avg_px']:.4f}",
                    str(v['sell']['no_trades'])], divider=True)
    for k,v in aggregation.items():
        if k != 'bank-a':
            table.add_row([k,
                            'buy',
                            f"{v['buy']['volume']:.4f}",
                            f"{v['buy']['avg_px']:.4f}",
                            str(v['buy']['no_trades'])])
            table.add_row(['',
                            'sell',
                            f"{v['sell']['volume']:.4f}",
                            f"{v['sell']['avg_px']:.4f}",
                            str(v['sell']['no_trades'])], divider=True)
    print(table)

if __name__ == "__main__":
    action_parse = parse_action (sys.argv[1]) if (len(sys.argv) == 2) else None
    if action_parse is None:
        print('demo of creating a trade by bank-a with counterparty')
        print(f'use: {os.path.basename(__file__)} action where action is B/Samt@pricefcounterparty')
        print('for example:')
        print(f'\tto buy 100 from bank-b at 102 --> {os.path.basename(__file__)} B100@102fbank-b')
        print(f'\tto sell 100 to bank-c at 102 --> {os.path.basename(__file__)} S100@102fbank-c')
    else:
        main(action_parse_in=action_parse)
        events = get_all_events()
        print_events (events)
# EOF
