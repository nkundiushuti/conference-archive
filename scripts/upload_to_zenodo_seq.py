#!/usr/bin/env python
"""Upload JSON databases to Zenodo.

To Use
------
You must set / export two environment variables for access to Zenodo;

```
export ZENODO_TOKEN_PROD=<PRIMARY_TOKEN>
export ZENODO_TOKEN_DEV=<SANDBOX_TOKEN>
```

Note: This script will yell loudly if the requested token is unset.

Now, you can then upload the sample data to the development site:
```
$ ./scripts/upload_to_zenodo.py \
    data/proceedings.json \
    data/conferences.json \
    uploaded-proceedings.json \
    --stage dev \
    --verbose 50 \
    --num_cpus -2 \
    --max_items 10
```
"""
import argparse
from joblib import Parallel, delayed
import json
import logging
import os
import random
import sys
import hashlib

### relative imports from parent
sys.path.insert(1, os.path.join(sys.path[0], '..'))
import zen

import zen.api
import zen.models

logger = logging.getLogger("upload_to_zenodo")


def upload(ismir_paper, conferences, stage=zen.DEV, old_zenodo=None, dry_run=False):
    """Upload a file / metadata pair to a Zenodo stage.

    Parameters
    ----------
    ismir_paper : zen.models.IsmirPaper
        ISMIR paper record.

    conferences : dict of zen.models.IsmirConference
        Conference metadata.

    stage : str
        One of [dev, prod]; defines the deployment area to use.

    Returns
    -------
    updated_paper : zen.models.IsmirPaper
        An updated IMSIR paper object.
    """
    ismir_paper = zen.models.IsmirPaper(**ismir_paper)
    conf = zen.models.IsmirConference(**conferences[ismir_paper['year']])

    if old_zenodo is not None:
        response = zen.edit(int(old_zenodo['zenodo_id']), stage=stage)
        if response.status_code >= 300:
            response = response.json()
            print(response)
            zid, response = zen.new_version_for_id(int(old_zenodo['zenodo_id']), stage=stage)
            new_version = True
        else:

            zid = int(old_zenodo['zenodo_id'])
            new_version = False

        response = response.json()
        checksum = response.get('files')[0].get('checksum')
        new_checksum = hashlib.md5(open(ismir_paper['ee'],'rb').read()).hexdigest()
        if new_version or checksum != new_checksum:
            version = 0
            filename = reponse.get('files')[0].get('checksum')
            if '_' in filename:
                version = int(filename.split('_')[-1].split('.pdf')[0])
            upload_response = zen.upload_file(zid, ismir_paper['ee'], version+1, stage=stage)
            ismir_paper['ee'] = upload_response['links']['download']
            print('new version')

        import pdb;pdb.set_trace()
    else:
        zid = zen.create_id(stage=stage)
        upload_response = zen.upload_file(zid, ismir_paper['ee'], stage=stage)
        ismir_paper['ee'] = upload_response['links']['download']
        print('create')


    # TODO: Should be a package function
    zenodo_meta = zen.models.merge(
        zen.models.Zenodo, ismir_paper, conf,
        creators=zen.models.author_to_creators(ismir_paper['author']),
        partof_pages=ismir_paper['pages'],
        description=ismir_paper['abstract'])

    zen.update_metadata(zid, zenodo_meta.dropna(), stage=stage)
    import pdb;pdb.set_trace()
    publish_response = zen.publish(zid, stage=stage)

    ismir_paper.update(doi=publish_response['doi'],
                       url=publish_response['doi_url'],
                       zenodo_id=zid)

    return ismir_paper


def archive(proceedings, conferences, stage=zen.DEV, num_cpus=-2, verbose=0, dry_run=False, output=None):
    final = []
    for paper in proceedings:
        old_zenodo = None
        if output is not None:
            old_zenodo_list = [o for o in output if o['title']==paper['title']]
            if len(old_zenodo_list)>0:
                old_zenodo = old_zenodo_list[0]
        res = upload(paper, conferences, stage, old_zenodo, dry_run)
        if verbose>0:
            print(res)
        final.append(res)
    return final


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(description=__doc__)

    # Inputs
    parser.add_argument("proceedings",
                        metavar="proceedings", type=str,
                        help="Path to JSON proceedings records.")
    parser.add_argument("conferences",
                        metavar="conferences", type=str,
                        help="Path to a JSON file of conference metadata.")
    parser.add_argument("output_file",
                        metavar="output_file", type=str,
                        help="Path to an output JSON file for writing updated records.")
    parser.add_argument("--stage",
                        metavar="stage", type=str, default=zen.DEV,
                        help="Stage to execute.")
    parser.add_argument("--num_cpus",
                        metavar="num_cpus", type=int, default=-2,
                        help="Number of CPUs to use in parallel.")
    parser.add_argument("--verbose",
                        metavar="verbose", type=int, default=0,
                        help="Verbosity level for joblib.")
    parser.add_argument("--max_items",
                        metavar="max_items", type=int, default=None,
                        help="Maximum number of items to upload.")
    parser.add_argument("--dry_run",
                        action='store_true',
                        help="Dry run (without uploading anything)")
    args = parser.parse_args()
    proceedings = json.load(open(args.proceedings)) # 'encoding' = 'utf-8' might need to be added based on the encoding
    conferences = json.load(open(args.conferences)) 
    if os.path.exists(args.output_file):
        output = json.load(open(args.output_file))
    else:
        output = None

    # Subsample for staging
    if args.max_items is not None:
        random.shuffle(proceedings)
        proceedings = proceedings[:args.max_items]

    results = archive(proceedings, conferences, args.stage, args.num_cpus, args.verbose, args.dry_run, output)

    with open(args.output_file, 'w') as fp:
        json.dump(results, fp, indent=2)

    sys.exit(0 if os.path.exists(args.output_file) else 1)
