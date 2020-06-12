import aiohttp as http
from bs4 import BeautifulSoup as DOM
import csv
import time
import asyncio
import itertools as it
from dataclasses import dataclass
from datetime import date


@dataclass
class Contact(object):
    surname: str
    house_nr: str
    street: str
    zipcode: str
    dob: date

    cdl_dat_row_indices_by_attr = {
        'surname': 1, 'house_nr': 5, 'street': 6,
        'zipcode': 13, 'dob': 27,
    }

    boe_row_indices_by_attr = {
        'surname': 6, 'house_nr': 9, 'street': 10,
        'zipcode': 14, 'dob': 15
    }

    @classmethod
    def from_row(cls, row, row_indices_by_attr=cls.cdl_dat_row_indices_by_attr):
        attr_names = ('surname', 'house_nr', 'street', 'zipcode', 'dob')
        attr_indices = {attr: argpos for argpos, attr in enumerate(attr_names)}
        attr_values = tuple(None for i in range(len(fields))
        for attr, index in row_indices_by_attr:
            if attr not in attr_indices:
                raise ValueError('invalid field')
            attr_values[attr_indices[attr]] = row[index]
        if None in attr_values:
            missing = (attr_names[i] for i, v in enumerate(attr_values)
                       if v is None)
            missing_enum = ', '.join(f"'{field}'" for field in missing)
            msg = f'row_indices_by_attr missing entries for fields {missing_enum}'
            raise ValueError(msg)
        return cls(*args)

    @classmethod
    def from_cdl_dat_row(cls, row):
        return cls.from_row(cls, row, cls.cdl_dat_row_indices_by_attr)

    @classmethod
    def from_boe_row(cls, row):
        return cls.from_row(cls, row, cls.boe_row_indices_by_attr)

    async def status(self, session):
        endpoint = 'https://www.monroecounty.gov/etc/voter/index.php'
        formdata = {
                'v[lname]': self.surname,
                'v[no]': self.house_nr,
                'v[sname]': self.street,
                'v[zip]': self.zip,
                'v[dobm]': self.dob.month,
                'v[dobd]': self.dob.day,
                'v[doby]': self.dob.year,
                }
        rsp = await session.post(endpoint, data=formdata)
        markup = DOM(await rsp.text(), 'lxml')
        summary = markup.select('div#voter')[0]
        regstat = summary['class'][0]
        children = list(markup.select('div#voter > div')[1].descendants)
        party = children[3].strip()
        try:
            ballot_stat = children[6].strip()
        except IndexError:
            ballot_stat = ''
        return (regstat, party, ballot_stat)


async def main(records, steno):
    from argparse import ArgumentParser
    parser = ArgumentParser()

    def boe_or_cdl(s):
        candidates = {k: k for k in ('boe', 'cdl')}
        return candidates[s]

    parser.add_argument('--schema', default='cdl', type=boe_or_cdl)
    args = parser.parse_args()
    fieldmap = (Contact.cdl_dat_row_indices_by_attr
                if args.schema == 'boe'
                else Contact.boe_row_indices_by_attr)

    async def write_status_annotated(record, ostrm, session):
        stat = await Contact(record).status(session)
        record += stat
        ostrm.writerow(record)


    from tqdm import tqdm
    max_in_flight = 128
    in_flight = set(())
    async with http.ClientSession() as s:
        for record in tqdm(records):
            if len(in_flight) >= max_in_flight:
                try:
                    done, pending = await (
                            asyncio.wait(in_flight,
                                         return_when=asyncio.FIRST_COMPLETED)
                            )
                    await done.pop()
                finally:
                    in_flight -= done
            in_flight |= {asyncio.ensure_future(write_status_annotated(record, steno, s))}
        if len(in_flight) > 0:
            await asyncio.wait(in_flight)


if __name__ == '__main__':
    from sys import stdout, stdin
    records = list(csv.reader(stdin))
    steno = csv.writer(stdout, dialect='unix')
    asyncio.run(main(records, steno))
