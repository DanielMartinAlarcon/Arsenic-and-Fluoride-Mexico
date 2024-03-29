# Arsenic and Fluoride in Mexico
This repository is the data analysis companion to the paper `Co-occurrence of arsenic and fluoride in drinking water sources in Mexico: Geographical data visualization`, by M. T. Alarcón Herrera, Daniel A. Martin-Alarcon, Mélida Gutiérrez, Liliana Reynoso Cuevas, Alejandra Martín, Diego A. Martínez, and Jochen Bundshuh.

This repository (repo) contains all the raw data needed to reconstruct the results and figures of the paper, along with the code that carries out that reconstruction.  The analysis is coded using the pipeline management package [Luigi](https://luigi.readthedocs.io/en/stable/index.html), so that all the analysis steps can be carried out automatically.  The computing environment needed for all of this to happen has been saved as a [Docker](https://www.docker.com/) image that can be either built from the accompanying Dockerfile or downloaded whole from this paper's [DockerHub repo](https://hub.docker.com/r/danielmartinalarcon/arsenic-and-fluoride-in-mexico). Read more about this project in the accompanying blog post on my website, [Reproducible data science with Docker and Luigi](https://www.martinalarcon.org/2019-06-08-reproducible-science/).

# Folder map

```
├── LICENSE
├── Makefile           // Makefile with commands like `make data` or `make train`
├── README.md          // The top-level README for developers using this project.
├── data
│   ├── external       // Data from third party sources (zipped maps of Mexico, INEGI town data)
│   ├── interim        // Intermediate data that has been transformed. Includes unzipped maps.
│   ├── processed      // The final, canonical data sets for mapping.
│   └── raw            // The original, immutable CONAGUA data.
│
├── docker             // Dockerfile and requirements.txt
│        
├── notebooks          // Jupyter notebooks that explore the processed datasets.
│
├── reports            // Generated analysis
│   └── figures        // Figures generated by this analysis
│
├── src                // Source code for use in this project.
│   ├── __init__.py    // Makes src a Python module
│   │
│   ├── data_tasks     // Scripts to unzip or generate data
│   │
│   └── visualization  // Scripts that generate maps
│
└── test_environment.py // Checks current python version.  For use with Make.
```

# How to recreate this analysis

Follow these instructions in your terminal.  For more detailed information on these commands, check out wsargent's [Docker Cheatsheet](https://github.com/wsargent/docker-cheat-sheet/#containers).

## Install Docker
[Instructions for Windows](https://docs.docker.com/docker-for-windows/)

[Instructions for Mac](https://docs.docker.com/docker-for-mac/)

## Clone GitHub repo

`$ git clone https://github.com/DanielMartinAlarcon/Arsenic-and-Fluoride-in-Mexico.git`

## Pull Docker image

`$ docker pull danielmartinalarcon/arsenic-and-fluoride-in-mexico`

This image can also be built from scratch, using the Dockerfile included in this repo. If you prefer to do that, `cd` into the `docker` directory and run `docker build -t [IMAGE_NAME] .` to create a new container called `IMAGE_NAME`. Don't forget the trailing period (`.`); it's important. Either way, use `docker image ls` to see the list of available images and to get the `IMAGE_NAME` if you didn't assign it. 

## Create a container and run it

`$ docker run --name [CONTAINER_NAME] -p 9999:8888 -v [local/path/Arsenic-and-Fluoride-in-Mexico]:/app [IMAGE_NAME]`

This command does several things, which you can leave in or take out as needed.

* `--name [CONTAINER_NAME]`: Names the new container.
* `-p 9999:8888`: Connects port 8888 in the container with your local port 9999, so that you can access the container through Jupyter (more on this below).
* `-v [local/path/Arsenic-and-Fluoride-in-Mexico]:/app`: Mounts the root folder of the github repo on your machine to the folder `app` in the container.  Changes in one folder will be reflected in the other.  More importantly, changes in the container will remain in the local folder after the container is closed or deleted.  The data generated in upcoming steps will thus remain in the folder for this GitHub repo on your local machine.

## Access your new container through Jupyter
If you used the `-v` tag above to mount the container, you'll get terminal output that looks something like this:
```
$ docker run --name mynewcontainer -p 9999:8888 -v /Users/DMA/Repos/Arsenic-and-Fluoride-in-Mexico:/app a3adbd6e39d3
[I 01:19:42.683 NotebookApp] Writing notebook server cookie secret to /root/.local/share/jupyter/runtime/notebook_cookie_secret
[I 01:19:43.116 NotebookApp] Serving notebooks from local directory: /app
[I 01:19:43.116 NotebookApp] The Jupyter Notebook is running at:
[I 01:19:43.116 NotebookApp] http://(0b1fc79228f6 or 127.0.0.1):8888/?token=d287729b6add165afafb674a0899ee6b3893ec9e42b25856
[I 01:19:43.116 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
[C 01:19:43.125 NotebookApp] 
    
    To access the notebook, open this file in a browser:
        file:///root/.local/share/jupyter/runtime/nbserver-1-open.html
    Or copy and paste one of these URLs:
        http://(0b1fc79228f6 or 127.0.0.1):8888/?token=d287729b6add165afafb674a0899ee6b3893ec9e42b25856
```
If you open `http://localhost:9999` in a browser on your local machine, you should see a Jupyter window prompting you for the token above (`d287729b6add165afafb674a0899ee6b3893ec9e42b25856` in this example). Provide the token and you'll be accessing your container directly through Jupyter.

## Open your container
Open a new terminal window (since the previous one is now hosting your open Jupyter session).  Use the following command to enter the running container.

`$ docker exec -it [CONTAINER_NAME] /bin/bash`

Look around, notice how the folder structure mirrors the one in the local folder that you mounted onto the container.

## Run the analysis
From inside the running container, in the root folder `app`, you can run several [make](https://www.gnu.org/software/make/) commands described in the project's Makefile.  The two most important are:

* `make data`: Commands Luigi to run the full analysis, generating all the processed data and all the figures of this project.  It takes ~10 min on my machine.
* `make data_clean`: Deletes all of the above.

A successful `make data` will produce the following output:
```
===== Luigi Execution Summary =====

Scheduled 13 tasks of which:
* 12 complete ones were encountered:
    - 1 AggregateNearbySites(radius=5)
    - 1 AllContaminantsMap()
    - 1 AllSitesMap()
    - 1 ArsenicAndFluorideMap()
    - 1 ArsenicMap()
    ...
* 1 ran successfully:
    - 1 FinalTask()

This progress looks :) because there were no failed tasks or missing dependencies
```

## Explore the results
You can find data generated by `make data` in `data/interim`, `data/processed`, and `reports/figures`. The folder `notebooks` contains two Jupyter notebooks that import the processed data and show the origin of a few summary statistics that are quoted in the manuscript.  

# Other resources
The folder structure for this project is based on [Cookiecutter Data Science with luigi](https://github.com/ffmmjj/luigi_data_science_project_cookiecutter), and the [Docker Cheat Sheet](https://github.com/wsargent/docker-cheat-sheet) provides very useful tips.
