from restkit import request, Resource, SimplePool
from restkit.errors import RequestFailed
import json
from datetime import datetime
from settings import settings

class GisgraphyResource(Resource):

    def __init__(self):
        Resource.__init__(self,
                settings.gisgraphy_url,
                pool_instance=SimplePool(keepalive=2),
                client_opts={'timeout':30},
        )

    def fulltext(self, q, headers=None, **kwargs):
        try:
            #we make the query lower case as workaround for "Portland, OR"
            r = self.get('fulltext/fulltextsearch',
                headers,
                q=q.lower(),
                format="json",
                **kwargs)
            return json.loads(r.body_string())["response"]["docs"]
        except RequestFailed as failure:
            print "%d while retieving url!"%failure.response.status_int
            print failure.response.final_url
            logging.error("%s while retrieving %s",
                    failure.response.status,
                    failure.response.final_url
            )
            raise

if __name__ == '__main__':
    res = GisgraphyResource()
    f = res.fulltext('Austin TX')


