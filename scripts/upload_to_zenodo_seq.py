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

    if not ismir_paper['zenodo_id']:
        # New submission
        zid = zen.create_id(stage=stage)
    else:
        # Update mode
        #  * If the checksum is different, re-upload the pdf
        #  * Update the metadata regardless
        publish_response = zen.new_version_for_id(zid, stage=stage)

    if old_zenodo is not None:
        edit_response = zen.edit(int(old_zenodo['zenodo_id']), stage=stage)
        message = ' \nA newer version of this paper has been uploaded at: https://zenodo.org/record/'+str(zid)
        old_zenodo_meta = zen.models.merge(
            zen.models.Zenodo, old_zenodo, conf,
            creators=zen.models.author_to_creators(ismir_paper['author']),
            partof_pages=ismir_paper['pages'],
            description=ismir_paper['abstract']+message)
        old_zenodo_meta['keywords'].append('obsolete')
        if 'doi' in old_zenodo_meta: del old_zenodo_meta['doi']
        if 'doi_url' in old_zenodo_meta: del old_zenodo_meta['doi_url']
        if 'zenodo_id' in old_zenodo_meta: del old_zenodo_meta['zenodo_id']
        import pdb;pdb.set_trace()
        zen.update_metadata(int(old_zenodo['zenodo_id']), old_zenodo_meta.dropna(), stage=stage)
        publish_response = zen.publish(int(old_zenodo['zenodo_id']), stage=stage)

    if not dry_run:
        upload_response = zen.upload_file(zid, ismir_paper['ee'], stage=stage)
        ismir_paper['ee'] = upload_response['links']['download']

        # TODO: Should be a package function
        zenodo_meta = zen.models.merge(
            zen.models.Zenodo, ismir_paper, conf,
            creators=zen.models.author_to_creators(ismir_paper['author']),
            partof_pages=ismir_paper['pages'],
            description=ismir_paper['abstract'])

        zen.update_metadata(zid, zenodo_meta.dropna(), stage=stage)
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
            old_zenodo = [o for o in output if o['title']==paper['title']]
            paper['zenodo_id'] = old_zenodo['zenodo_id']
        res = upload(paper, conferences, stage, old_zenodo[0], dry_run)
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
