# Copyright 2022 Khaled Hassan <khaled.hassan@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Implements Apex Crypto (via Titan)

Apex Crypto gain/loss output provides already-reconciled transactions, i.e.,
each buy/sell pair comes in a single record, on a single line.

Does not handle:
* ???
"""

import csv
from datetime import datetime
from decimal import Decimal
import re
import utils


FIRST_LINE = 'ACCOUNT_ID,TAX_YEAR,SUBLOT_ID,SECNO,CUSIP,SYMBOL,SEC_DESCR,SEC_TYPE,SEC_SUBTYPE,SUBACCOUNT_TYPE,OPEN_TRAN_ID,CLOSE_TRAN_ID-SEQNO,OPEN_DATE,CLOSE_DATE,CLOSE_EVENT,DISPOSAL_METHOD,QUANTITY,LONG_SHORT_IND,NO_WS_COST,NO_WS_PROCEEDS,NO_WS_GAINLOSS,WS_COST_ADJ,WS_PROC_ADJ,WS_LOSS_ID-SEQNO,1099_ACQ_DATE,1099_DISP_DATE,1099_COST,1099_PROCEEDS,GROSS_NET_IND,TOTAL_GAINLOSS,ORDINARY_GAINLOSS,1099_DISALLOWED_LOSS,1099_MARKET_DISCOUNT,8949_GAINLOSS,8949_CODE,HOLDING_DATE,TERM,COVERED_IND,8949_BOX,1099_1256_CY_REALIZED,1099_1256_PY_UNREALIZED,1099_1256_CY_UNREALIZED,1099_1256_AGGREGATE\n'

TRANSACTION_TYPE = 'Trans type'


class ApexCrypto:
    @classmethod
    def name(cls):
        return "Apex Crypto"

    @classmethod
    def washSaleDisallowedAmount(cls, dict):
        """Returns wash sale disallowed amount"""
        value = dict['1099_DISALLOWED_LOSS'].rstrip()
        if value == '$0.00':
            return None
        else:
            return Decimal(value.replace(',', '').replace('$', ''))

    @classmethod
    def buyDate(cls, dict):
        """Returns date of transaction as datetime object."""
        # Our input date format is YYYY-MM-DD.
        return datetime.strptime(dict['OPEN_DATE'], '%Y-%m-%d')

    @classmethod
    def sellDate(cls, dict):
        """Returns date of transaction as datetime object."""
        # Our input date format is YYYY-MM-DD.
        return datetime.strptime(dict['CLOSE_DATE'], '%Y-%m-%d')

    @classmethod
    def isShortTerm(cls, dict):
        timedelta = cls.sellDate(dict) - cls.buyDate(dict)
        return timedelta.years <= 1

    @classmethod
    def symbol(cls, dict):
        return dict['SYMBOL']

    @classmethod
    def numShares(cls, dict):
        return Decimal(dict['QUANTITY'])

    @classmethod
    def costBasis(cls, dict):
        # TODO: not sure if commas are used in the input file, but the schwab
        # parser had this, so I'll keep it.
        #
        # Cost amount may include commas as thousand separators, which
        # Decimal does not handle. Remove any dollar signs if present.
        return Decimal(dict['1099_COST'].replace(',', '').replace('$', ''))

    @classmethod
    def saleProceeds(cls, dict):
        # TODO: not sure if commas are used in the input file, but the schwab
        # parser had this, so I'll keep it.
        #
        # Proceeds amount may include commas as thousand separators, which
        # Decimal does not handle. Remove any dollar signs if present.
        return Decimal(dict['1099_PROCEEDS'].replace(',', '').replace('$', ''))

    @classmethod
    def isFileForBroker(cls, filename):
        with open(filename) as f:
            first_line = f.readline()
            return first_line == FIRST_LINE

    @classmethod
    def parseFileToTxnList(cls, filename, tax_year):
        YEAR_BEGIN = datetime(tax_year, 1, 1)
        YEAR_END = datetime(tax_year, 12, 31)
        buy_date = YEAR_BEGIN
        sell_date = YEAR_END

        txns = csv.reader(open(filename), delimiter=',', quotechar='"')
        line_num = 0
        txn_list = []
        names = None

        for row in txns:
            line_num = line_num + 1
            if line_num == 1:
                names = row
                continue

            txn_dict = {}
            for i in range(0, len(names)):
                txn_dict[names[i]] = row[i] # match header column names to each row

            curr_txn = utils.Transaction()

            adjustment = cls.washSaleDisallowedAmount(txn_dict)
            if not adjustment:
                curr_txn.adjustment = adjustment

            curr_txn.desc = '%s %s' % (
                cls.numShares(txn_dict), cls.symbol(txn_dict))
            #curr_txn.desc = cls.symbol(txn_dict)

            curr_txn.costBasis = cls.costBasis(txn_dict)
            curr_txn.buyDate = cls.buyDate(txn_dict)

            if curr_txn.buyDate == 'Various':
                curr_txn.buyDate = buy_date
                curr_txn.buyDateStr = 'Various'
            else:
                curr_txn.buyDateStr = utils.txfDate(curr_txn.buyDate)

            curr_txn.saleProceeds = cls.saleProceeds(txn_dict)
            curr_txn.sellDate = cls.sellDate(txn_dict)

            if curr_txn.sellDate == 'Various':
                curr_txn.sellDate = sell_date
                curr_txn.sellDateStr = 'Various'
            else:
                curr_txn.sellDateStr = utils.txfDate(curr_txn.sellDate)


            # The Apex Crypto CSV file actually tells us which box to use for form 8949, so use that value
            # to apply the correct entryCode.
            code_map = {"A": 321, "B": 711, "C": 712, "D": 323, "E": 713, "F": 714}
            curr_txn.entryCode = code_map[txn_dict['8949_BOX'][0].upper()]

            if tax_year and curr_txn.sellDate.year != tax_year:
                utils.Warning('ignoring txn: "%s" (line %d) as the sale is not from %d\n' %
                              (curr_txn.desc, line_num, tax_year))
                continue

            txn_list.append(curr_txn)

        return txn_list
