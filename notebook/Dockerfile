# JupyterLab for exploring and developing with Spark (notebook profile).
# Built on our Spark image: same versions and same Python (3.10) as the jobs
# and the cluster, so what works here works in a job.
FROM datalake/spark:4.1.4

USER root
RUN python3 -m pip install --no-cache-dir \
      "jupyterlab==4.6.*" "pandas<3" pyarrow matplotlib "trino==0.340.*" \
 # Makes the PySpark in /opt/spark importable from Python (spark-submit does
 # this on its own for jobs; Jupyter does not).
 && printf '%s\n' /opt/spark/python /opt/spark/python/lib/py4j-*-src.zip \
      > "$(python3 -c 'import site; print(site.getsitepackages()[0])')/pyspark.pth" \
 # User with the same uid as the owner of the notebooks/ folder on the host
 # (1000), so saved notebooks stay editable outside the container.
 && useradd --uid 1000 --create-home --shell /bin/bash analyst \
 && mkdir -p /home/analyst/notebooks \
 && chown analyst:analyst /home/analyst/notebooks

USER analyst
WORKDIR /home/analyst/notebooks
ENV SPARK_USER=analyst
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", \
     "--ServerApp.root_dir=/home/analyst/notebooks"]
