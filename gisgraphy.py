import json
import re

from restkit import request, Resource, SimplePool
from restkit.errors import RequestFailed

from settings import settings
from models import GeonamesPlace


class GisgraphyResource(Resource):
    COORD_RE = re.compile('(-?\d+\.\d+), *(-?\d+\.\d+)')

    def __init__(self):
        Resource.__init__(self,
                settings.gisgraphy_url,
                pool_instance=SimplePool(keepalive=2),
                client_opts={'timeout':30},
        )

    def fulltextsearch(self, q, headers=None, **kwargs):
        #we make the query lower case as workaround for "Portland, OR"
        r = self.get('fulltext/fulltextsearch',
            headers,
            q=q.strip(),
            format="json",
            spellchecking=False,
            **kwargs)
        return json.loads(r.body_string())["response"]["docs"]


    def twitter_loc(self, q):
        if not q:
            return None
        q = q.lower()
        # check for "30.639, -96.347" style coordinates
        match = self.COORD_RE.search(q)
        if match:
            return GeonamesPlace(
                lat=float(match.group(1)),
                lng=float(match.group(2)),
                feature_code='COORD',
            )
        results = self.fulltextsearch(q)
        # Is there a local place in the first 10 results?
        for place in results:
            if self.in_local_box(place):
                return GeonamesPlace(place)
        # otherwise, return the first result
        if len(results)>0:
            return GeonamesPlace(results[0])
        # try splitting q in half
        for splitter in ('/','-','and','or'):
            parts = q.split(splitter)
            if len(parts)==2:
                for part in parts:
                    res = self.twitter_loc(part)
                    if res:
                        return res
        return None

    def in_local_box(self, place):
        box = settings.local_box
        return all(box[d][0]<place[d]<box[d][1] for d in ('lat','lng'))


if __name__ == '__main__':
    res = GisgraphyResource()
    f = res.fulltextsearch('Austin TX')
