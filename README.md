<p align="center">
  <img src="docs/kids_first_logo.svg" alt="Kids First repository logo" width="660px" />
</p>
<p align="center">
  <a href="https://github.com/kids-first/kf-template-repo/blob/master/LICENSE"><img src="https://img.shields.io/github/license/kids-first/kf-template-repo.svg?style=for-the-badge"></a>
</p>

# Kids First Repository Template

Use this template to bootstrap a new Kids First repository.

## Badges

Update the LICENSE badge to point to the new repo location on GitHub.
Note that the LICENSE badge will fail to render correctly unless the repo has
been set to **public**.

Add additional badges for CI, docs, and other integrations as needed within the
`<p>` tag next to the LICENSE.

## Repo Description

Update the repositories description with a short summary of the repository's
intent.
Include an appropriate emoji at the start of the summary.

Add a handful of tags that summarize topics relating to the repository.
If the repo has a documentation site or webpage, add it next to the repository
description.

## Run the ETL locally to update QA data

1. Get a fresh cookie and paste it in ***.conf file

2. Run Fhavro task (see its README for args and env variables)

3. Run Import task (see its README for args and env variables)

4. Run Prepare index task all (see its README for args and env variables)

5. Check that you upload the last version of indices template in Minio/S3

6. If templates changed, delete the previous one in ES : 

```
DELETE _index_template/template_[study | participant | file | biospecimen]_centric
```

7. Run Index task for study (see its README for args and env variables)

8. Run Index task for participant (see its README for args and env variables)

9. Run Index task for file (see its README for args and env variables)

10. Run Index task for biospecimen (see its README for args and env variables)

11. Make sure data is correctly loaded in ES, also check mapping for the 4 new indices, there should not be any `"type": "text"`.

12. Switch aliases to add the new indices and remove the old ones

13. Run arranger (not the wrapper) locally 

14. Open a browser on this endpoint `/admin/graphql`

15. Create a new project

```
mutation newProject {
  newProject(id: "[YYYY_MM_DD]_v[X]"){
    id
    active
    timestamp
  }
}
```

16. Create new indices

```
mutation newIndexStudy {
  newIndex(projectId: "[NEW_PROJECT_ID]", graphqlField:"study", esIndex: "study_centric") {
    id
  }
}

mutation newIndexParticipant {
  newIndex(projectId: "[NEW_PROJECT_ID]", graphqlField:"participant", esIndex: "participant_centric") {
    id
  }
}

mutation newIndexFile {
  newIndex(projectId: "[NEW_PROJECT_ID]", graphqlField:"file", esIndex: "file_centric") {
    id
  }
}

mutation newIndexBiospecimen {
  newIndex(projectId: "[NEW_PROJECT_ID]", graphqlField:"biospecimen", esIndex: "biospecimen_centric") {
    id
  }
}
```

17. Fix mapping

```
mutation updateStudyMappingTypeOfOmics($inputTypeOfOmics:ExtendedFieldMappingInput!) {
  updateExtendedMapping(projectId: "[NEW_PROJECT_ID]", graphqlField:"study", field: "type_of_omics", extendedFieldMappingInput: $inputTypeOfOmics) {
    field
    isArray
  }
}

mutation updateStudyMappingExperimentalStrategy($inputExperimentalStrategy:ExtendedFieldMappingInput!) {
  updateExtendedMapping(projectId: "[NEW_PROJECT_ID]", graphqlField:"study", field: "experimental_strategy", extendedFieldMappingInput: $inputExperimentalStrategy) {
    field
    isArray
  }
}

mutation updateStudyMappingDataAccess($inputDataAccess:ExtendedFieldMappingInput!) {
  updateExtendedMapping(projectId: "[NEW_PROJECT_ID]", graphqlField:"study", field: "data_access", extendedFieldMappingInput: $inputDataAccess) {
    field
    isArray
  }
}

mutation updateStudyMappingMondoParents($inputMondoParents:ExtendedFieldMappingInput!) {
  updateExtendedMapping(projectId: "[NEW_PROJECT_ID]", graphqlField:"participant", field: "mondo.parents", extendedFieldMappingInput: $inputMondoParents) {
    field
    isArray
  }
}

mutation updateStudyMappingObservedPhenotypeParents($inputObservedPhenotypeParents:ExtendedFieldMappingInput!) {
  updateExtendedMapping(projectId: "[NEW_PROJECT_ID]", graphqlField:"participant", field: "observed_phenotype.parents", extendedFieldMappingInput: $inputObservedPhenotypeParents) {
    field
    isArray
  }
}

mutation updateStudyMappingObservedPhenotypeAgeAtEventDays($inputObservedPhenotypeAgeAtEventDays:ExtendedFieldMappingInput!) {
  updateExtendedMapping(projectId: "[NEW_PROJECT_ID]", graphqlField:"participant", field: "observed_phenotype.age_at_event_days", extendedFieldMappingInput: $inputObservedPhenotypeAgeAtEventDays) {
    field
    isArray
  }
}
```

With these inputs (isArray true is the important part)

```
{
  "inputTypeOfOmics": {
    "displayName": "Type Of Omics",
    "active": false,
    "isArray": true,
    "primaryKey": false,
    "quickSearchEnabled": false
  },
  "inputExperimentalStrategy": {
    "displayName": "Experimental Strategy",
    "active": false,
    "isArray": true,
    "primaryKey": false,
    "quickSearchEnabled": false
  },
  "inputDataAccess": {
    "displayName": "Data Access",
    "active": false,
    "isArray": true,
    "primaryKey": false,
    "quickSearchEnabled": false
  },
   "inputMondoParents": {
    "displayName": "Mondo Parents",
    "active": false,
    "isArray": true,
    "primaryKey": false,
    "quickSearchEnabled": false
  },
  "inputObservedPhenotypeParents": {
    "displayName": "Observed Phenotype Parents",
    "active": false,
    "isArray": true,
    "primaryKey": false,
    "quickSearchEnabled": false
  },
  "inputObservedPhenotypeAgeAtEventDays": {
    "displayName": "Observed Phenotype Age At Event Days",
    "active": false,
    "isArray": true,
    "primaryKey": false,
    "quickSearchEnabled": false
  }
}
```

18. Restart Arranger 

19. Update portal `REACT_APP_ARRANGER_PROJECT_ID = [NEW_PROJECT_ID]` in Netlify and Retry last deploy
