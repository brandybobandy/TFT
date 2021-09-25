from diagrams import Cluster, Diagram
from diagrams.generic.place import Datacenter
from diagrams.generic.database import SQL
from diagrams.programming.language import Python

with Diagram(show=False):

    python = Python("Python")

    with Cluster("Source"):
        [Datacenter("Riot API")] >> python

    with Cluster("Destination"):
        sql = [SQL("PostgreSQL")]

    python >> sql

    