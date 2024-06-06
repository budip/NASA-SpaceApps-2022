<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
## Table of Contents *generated with [DocToc](https://github.com/thlorenz/doctoc)*
- [Development](#development)
- [Modifying Parameters](#modifying-parameters)
- [GitLab CI](#gitlab-ci)
- [Troubleshooting](#troubleshooting)
- [Members](#members)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->


## Development
**NOTE**: the original project was commited to a community GitLab ([here](https://gitlab.com/godotuniverse/give-the-sun-a-voice)) to be submitted to the panelist for final competition. For local development and testing, please clone and deploy from GitHub as explained below.


Pre-requisite:
1. Valid account in GitLab.
2. Personal Access Token (PAT) create under this user's account.
3. Administrator or similar level in local environment.
4. This sample is using Visual Studio Code for IDE.

Run project locally:
1. Clone from this repository: https://github.com/budip/NASA-SpaceApps-2022.git.
2. Open this file using IDE: `nasa_spaceApp/goes.py.`
3. **Using VSCode**: select `Run and Debug` icon in the left navigation and click Debug.
4. **Using Terminal**: open the terminal and go to `nasa_spaceApp` directory. Then type python3 `goes.py`.
5. Files will be generated inside of `data` directory.

## Modifying Parameters
`goes.py` has 2 main methods that are ready to be executed with the default values.


### --> By Date <--
Method: get_goes_by_date()

Function: retrieve a specific data from GOES 16, 17 or 18 satellites in a specific date.

Modification:
- Open `goes.py` file.
- Fine 3 statements below and modify the input parameters.
- Remove the comment from this line: `get_geos_by_date()`.
- Execute the program.

```
satList  = [16, 17, 18]
typeList = ['xrs-x1s']
dateList = ['2022-10-02', '2022-10-01']
```

Results:
- Source: `https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/goes17/l2/data/xrsf-l2-flx1s_science/2022/10/sci_xrsf-l2-flx1s_g17_d20221002_v2-1-0.nc`
- Output: `nasa_spaceApp/data/goes-17-xrs-x1s-20220927.json`
- JSON: ```{"date":{"0":1663632000253,"1":1663632001253,"2":1663632002253,"3":1663632003253,"4":1663632004253,"5":1663632005253,"6":1663632006253,"7":1663632007253,"8":1663632008253,"9":1663632009253,"10":1663632010253,"11":1663632011253,"12":1663632012253,"13":1663632013253,...```


### --> Last 7 Days <--
Method: get_goes_last_7d_complete()

Function: retrive dataset from several instruments from GOES satellites for the past 7 days.

Modification:
- Not needed

Results:
- Source: `https://services.swpc.noaa.gov/json/goes/primary/xray-flares-7-day.json`
- Output: `nasa_spaceApp/data/7-day/xrays-flares.json`
- JSON: ```[
  {
    "time_tag": "2022-09-26T08:48:00Z",
    "begin_time": "2022-09-26T08:48:00Z",
    "begin_class": "C1.0",
    "max_time": "2022-09-26T09:11:00Z",
    "max_class": "C4.7",
    "max_xrlong": 4.7267981244658586e-06,
    "max_ratio": 5.888602034978241,
    "max_ratio_time": "2022-09-26T09:04:58Z",
    "current_int_xrlong": 0.009434806182980537,
    "end_time": "2022-09-26T09:36:00Z",
    "end_class": "C2.8",
    "satellite": 16
  },...```


## GitLab CI
This project's static Pages are built by [GitLab CI][ci], following the steps
defined in [`.gitlab-ci.yml`](.gitlab-ci.yml):

```
image: alpine:latest

pages:
  stage: deploy
  script:
  - echo 'Nothing to do...'
  artifacts:
    paths:
    - public
  only:
  - master
```

The above example expects to put all your HTML files in the `public/` directory.

## Troubleshooting
1. CSS is missing! That means that you have wrongly set up the CSS URL in your
   HTML files. Have a look at the [index.html] for an example.

[ci]: https://about.gitlab.com/gitlab-ci/
[index.html]: https://gitlab.com/pages/plain-html/blob/master/public/index.html
[userpages]: https://docs.gitlab.com/ce/user/project/pages/introduction.html#user-or-group-pages
[projpages]: https://docs.gitlab.com/ce/user/project/pages/introduction.html#project-pages


## Members
- Aaren Lai
- Budi Prasetya
- Nathan Hicks
- Shira Rubin
- Sunshine Eversull
- Zach Syver

