import aiohttp as http
from bs4 import BeautifulSoup as DOM
import csv
import time
import asyncio
import itertools as it


class Contact(object):
    def __init__(self, ent):
        self.surname = ent[1]
        self.house_nr = ent[5]
        self.street = ent[6]
        self.zip = ent[13]
        self.dob = tuple(ent[27].split('/', 3))

    async def status(self, session):
        endpoint = 'https://www.monroecounty.gov/etc/voter/index.php'
        formdata = {
                'v[lname]': self.surname,
                'v[no]': self.house_nr,
                'v[sname]': self.street,
                'v[zip]': self.zip,
                'v[dobm]': self.dob[0],
                'v[dobd]': self.dob[1],
                'v[doby]': self.dob[2],
                }
        rsp = await session.post(endpoint, data=formdata)
        markup = DOM(await rsp.text(), 'lxml')
        summary = markup.select('div#voter')[0]
        regstat = summary['class'][0]
        children = list(markup.select('div#voter > div')[1].descendants)
        party = children[3].strip()
        ballot_stat = children[6].strip()
        return (regstat, party, ballot_stat)


async def write_status_annotated(record, ostrm, session):
    stat = await Contact(record).status(session)
    record += stat
    ostrm.writerow(record)


async def main(records, steno):
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
        await asyncio.wait(in_flight)


if __name__ == '__main__':
    from sys import stdout, stdin
    records = list(csv.reader(stdin))
    steno = csv.writer(stdout, dialect='unix')
    asyncio.run(main(records, steno))
