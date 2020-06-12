import aiohttp as http
from bs4 import BeautifulSoup as DOM
import csv
import time
import asyncio
import itertools as it
import dataclasses
from datetime import date


@dataclasses.dataclass
class Contact(object):
    surname: str
    house_nr: str
    street: str
    zipcode: str
    dob: date

    cdl_dat_schema = {
        'surname': 1, 'house_nr': 5, 'street': 6,
        'zipcode': 13, 'dob': 27,
    }

    boe_schema = {
        'surname': 6, 'house_nr': 9, 'street': 10,
        'zipcode': 14, 'dob': 15
    }

    @classmethod
    def from_row(cls, row, schema):
        attr_fields = dataclasses.fields(cls)
        attr_indices = {attr: argpos for argpos, attr
                        in enumerate(f.name for f in attr_fields)}
        attr_values = [None for i in range(len(attr_indices))]
        for attr, index in schema.items():
            if attr not in attr_indices:
                raise ValueError('invalid field')
            cell = row[index]
            if attr == 'dob':
                month, day, year = map(int, cell.split('/', 3))
                attr_values[attr_indices[attr]] = date(year, month, day)
                continue
            attr_values[attr_indices[attr]] = cell
        if None in attr_values:
            missing = (attr_fields[i].name for i, v in enumerate(attr_values)
                       if v is None)
            missing_enum = ', '.join(f"'{field}'" for field in missing)
            msg = (r'row_indices_by_attr missing entries '
                   f'for fields {missing_enum}')
            raise ValueError(msg)
        return cls(*attr_values)

    async def status(self, session):
        endpoint = 'https://www.monroecounty.gov/etc/voter/index.php'
        formdata = {
                'v[lname]': self.surname,
                'v[no]': self.house_nr,
                'v[sname]': self.street,
                'v[zip]': self.zipcode,
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
        ballot_stat = ('' if 6 not in range(len(children))
                       else children[6].strip())
        return (regstat, party, ballot_stat)


async def main(records, steno):
    from argparse import ArgumentParser
    parser = ArgumentParser()

    def boe_or_cdl(s):
        candidates = {k: k for k in ('boe', 'cdl')}
        return candidates[s]

    parser.add_argument('--schema', default='cdl', type=boe_or_cdl)
    args = parser.parse_args()
    schema = (Contact.boe_schema
              if args.schema == 'boe'
              else Contact.cdl_dat_schema)

    async def write_status_annotated(record, ostrm, session):
        try:
            contact = Contact.from_row(record, schema)
        except ValueError:
            return
        stat = await contact.status(session)
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
            in_flight |= {
                asyncio.ensure_future(write_status_annotated(record, steno, s))
            }
        if len(in_flight) > 0:
            await asyncio.wait(in_flight)


if __name__ == '__main__':
    from sys import stdout, stdin
    records = list(csv.reader(stdin))
    steno = csv.writer(stdout, dialect='unix')
    asyncio.run(main(records, steno))
