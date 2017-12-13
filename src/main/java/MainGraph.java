package org.neo4j.examples;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.io.fs.FileUtils;

import static org.neo4j.helpers.collection.Iterators.loop;

public class MainGraph
{
    private static final File databaseDirectory = new File( "target/neo4j-store-with-new-indexing" );

    //The relationship weights are like percentages that the jobs are alike (completely arbitrary)
    //If the company is the same, the job is 25% similar
    //If the job title is the same, the job is 80% similar
    private static final double DEFAULT_RELATIONSHIP_WEIGHT = 0;
    private static final double COMPANY_RELATIONSHIP_WEIGHT = 0.25;
    private static final double JOBTITLE_RELATIONSHIP_WEIGHT = 0.8;

    private static final double DEFAULT_NODE_WEIGHT = 10;
    private static final int DEFAULT_CLICK_INCREMENT = 1;
    private static final int DEFAULT_LIKE_INCREMENT = 10;
    private static final int DEFAULT_DISLIKE_DECREMENT = -10;

    //Database object
    GraphDatabaseService graphDb;
    //Relationships
    private static enum RelTypes implements RelationshipType
    {
        LIKE
    }

    //The example makes everything static but I just think it's simpler to use instance methods
    //I would be interested to hear why everything is actually static, most people's examples online use static methods
    public MainGraph()
    {
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( databaseDirectory );
    }


    public void createIndexDefinition(String label, String property)
    {
        // START SNIPPET: createIndex
        IndexDefinition indexDefinition;
        try ( Transaction tx = graphDb.beginTx() )
        {
            Schema schema = graphDb.schema();
            //These index things are just pointers that make searching the database faster
            //They make it faster to read and slower to write, also they cost storage
            indexDefinition = schema.indexFor( Label.label( label ) )
                    .on( property )
                    .create();
            tx.success();
        }
        // END SNIPPET: createIndex
        // START SNIPPET: wait
        try ( Transaction tx = graphDb.beginTx() )
        {
            Schema schema = graphDb.schema();
            schema.awaitIndexOnline( indexDefinition, 10, TimeUnit.SECONDS );
            tx.success();
        }
        // END SNIPPET: wait
        // START SNIPPET: progress
        try ( Transaction tx = graphDb.beginTx() )
        {
            Schema schema = graphDb.schema();
            System.out.println( String.format( "Percent complete: %1.0f%%",
                    schema.getIndexPopulationProgress( indexDefinition ).getCompletedPercentage() ) );
            tx.success();
        }
        // END SNIPPET: progress
    }

    public void createJobNode(String company, String jobTitle)
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            Label jobLabel = Label.label( "Job" );
            Node newNode = graphDb.createNode(jobLabel);

            //The primary key is a composite key of company and jobTitle, and since you can't access two attributes at once
            //I just combined the company and jobTitle into one attribute so that I can easily search for unique nodes.
            newNode.setProperty("jobID", company + ": " + jobTitle);

            newNode.setProperty("company", company);
            newNode.setProperty("jobTitle", jobTitle);
            newNode.setProperty("weight", DEFAULT_NODE_WEIGHT);

            //The users node might be changed to getAllNodes()
            //Create relationship between this node and all the other nodes
            try (ResourceIterator<Node> users = graphDb.findNodes(jobLabel))
            {
                while (users.hasNext())
                {
                    Node otherNode = users.next();
                    if(!otherNode.equals(newNode))
                    {
                        Relationship newRelationship = newNode.createRelationshipTo(otherNode, RelTypes.LIKE);

                        //The company and job cant both match because that would be the same exact node
                        if(newNode.getProperty("company").equals(otherNode.getProperty("company")))
                        {
                            newRelationship.setProperty("weight", COMPANY_RELATIONSHIP_WEIGHT);
                        }
                        else if(newNode.getProperty("jobTitle").equals(otherNode.getProperty("jobTitle")))
                        {
                            newRelationship.setProperty("weight", JOBTITLE_RELATIONSHIP_WEIGHT);
                        }
                        else
                        {
                            newRelationship.setProperty("weight", DEFAULT_RELATIONSHIP_WEIGHT);
                        }
                    }
                }
                users.close();
            }

            tx.success();
        }
    }

    /*public Node getJobNode(String company, String jobTitle)
    {
        //Initializing things to null is bad so i wont bother
        Node node = null;
        Label jobLabel = Label.label( "Job" );
        String desiredJobID = company + ": " + jobTitle;

        try ( Transaction tx = graphDb.beginTx();
              ResourceIterator<Node> nodes = graphDb.findNodes( jobLabel, "jobID", desiredJobID) )
        {
            //Finds the first node with the right ID (there should only be one)
            if ( nodes.hasNext() )
            {
                node = nodes.next();
                System.out.println("Retrieved node: " + node);
            }else {
                System.out.println("No node could be found for: " + desiredJobID);
            }

            nodes.close(); //Remember to close these things...?
        }

        return node;
    }*/

    //Updates the weight of this node and all the nodes that are like it
    public void updateWeightsAround(Node node, double increment)
    {
        //System.out.println("Updating weights around node: " + node.getProperty("jobID"));

        try ( Transaction tx = graphDb.beginTx() )
        {
            double oldWeight = ((Double)node.getProperty("weight"));
            //System.out.println("Previous weight: " + oldWeight);
            node.setProperty("weight", oldWeight + increment);
            //System.out.println("New weight: " + node.getProperty("weight"));

            Iterator<Relationship> relationships = node.getRelationships().iterator();
            while (relationships.hasNext())
            {
                Relationship relationship = relationships.next();
                if((Double)relationship.getProperty("weight") != 0)
                {
                    //System.out.println("Relationship weight: " + relationship.getProperty("weight"));
                    Node otherNode = relationship.getOtherNode(node);

                    //System.out.println("UPDATING NODE: " + otherNode.getProperty("jobID"));
                    //System.out.println("PREVIOUS NODE WEIGHT: " + otherNode.getProperty("weight"));

                    double relationshipWeight = (double)(relationship.getProperty("weight"));
                    double newWeight = ((Double)otherNode.getProperty("weight")) + (increment*relationshipWeight);
                    otherNode.setProperty("weight", newWeight);

                    //System.out.println("NEW WEIGHT: " + otherNode.getProperty("weight"));
                }
            }
            tx.success();
        }
    }

    //0 is a click
    //1 is a like
    //2 is a dislike
    public void clickOnJob(String company, String jobTitle, int typeOfClick)
    {
        Label jobLabel = Label.label( "Job" );
        String desiredJobID = company + ": " + jobTitle;

        try ( Transaction tx = graphDb.beginTx();
              ResourceIterator<Node> nodes = graphDb.findNodes( jobLabel, "jobID", desiredJobID) )
        {
            //Finds the first node with the right ID (there should only be one)
            if ( nodes.hasNext() )
            {
                Node node = nodes.next();
                switch(typeOfClick)
                {
                    case 0:
                        updateWeightsAround(node, DEFAULT_CLICK_INCREMENT);
                        break;
                    case 1:
                        updateWeightsAround(node, DEFAULT_LIKE_INCREMENT);
                        break;
                    case 2:
                        updateWeightsAround(node, DEFAULT_DISLIKE_DECREMENT);
                        break;
                }
                //System.out.println("WELL try this: " + node.getProperty("weight"));
            }else {
                System.out.println("There is no job for: " + desiredJobID);
            }

            nodes.close(); //Remember to close these things...?
            tx.success();
        }


    }

    public void deleteNodesAndRelationships()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            Label jobLabel = Label.label( "Job" );
            String desiredJobTitle = "Systems Analyst";

            //.println("GATES");

            for( Relationship relationship : graphDb.getAllRelationships())
            {
                relationship.delete();
            }
            for ( Node node : loop( graphDb.findNodes( jobLabel, "jobTitle", desiredJobTitle ) ) )
            {
                node.delete();
            }
            tx.success();
        }
    }

    public void printNodesInOrderOfWeight()
    {
        Label jobLabel = Label.label("Job");
        try ( Transaction tx = graphDb.beginTx();
              ResourceIterator<Node> nodes = graphDb.findNodes(jobLabel) )
        {
            ArrayList<Node> sortedNodes = new ArrayList<Node>();
            sortedNodes.add(nodes.next());
            while( nodes.hasNext() )
            {
                Node node = nodes.next();

                //System.out.println(((Integer)node.getProperty("weight")).doubleValue());

                int i = 0;
                while( i < sortedNodes.size() && ((Double)node.getProperty("weight"))
                        < ((Double)(sortedNodes.get(i).getProperty("weight"))) )
                {
                    i++;
                }

                sortedNodes.add(i, node);
            }

            System.out.println("----------ALL NODE WEIGHTS----------");

            for(int i = 0; i < sortedNodes.size(); i++)
            {
                System.out.println(sortedNodes.get(i).getProperty("weight") + " - " + sortedNodes.get(i).getProperty("jobID"));
            }

            nodes.close(); //Remember to close these things...?
            tx.success();
        }
    }

    public static void main( final String[] args ) throws IOException
    {
        System.out.println( "Starting database ..." );
        FileUtils.deleteRecursively( databaseDirectory );

        MainGraph mainGraph = new MainGraph();

        mainGraph.createIndexDefinition("Job", "weight");

        //mainGraph.createIndexDefinition("Job", "company");
        //mainGraph.createIndexDefinition("Job", "jobTitle");

        mainGraph.createJobNode("Boeing", "Business Analyst");
        mainGraph.createJobNode("Boeing", "IT");
        mainGraph.createJobNode("Boeing", "Electrical Engineer");
        mainGraph.createJobNode("Boeing", "Mechanical Engineer");
        mainGraph.createJobNode("Boeing", "Aerospace Engineer");
        mainGraph.createJobNode("Boeing", "Chief Electrical Engineer");
        mainGraph.createJobNode("Boeing", "Computer Engineer");

        mainGraph.createJobNode("Google", "Systems Analyst");
        mainGraph.createJobNode("Google", "Software Engineer");
        mainGraph.createJobNode("Google", "IT");
        mainGraph.createJobNode("Google", "Database Manager");
        mainGraph.createJobNode("Google", "Senior Developer");
        mainGraph.createJobNode("Google", "Web Developer");

        mainGraph.createJobNode("Microsoft", "Software Engineer");
        mainGraph.createJobNode("Microsoft", "IT");
        mainGraph.createJobNode("Microsoft", "Database Manager");
        mainGraph.createJobNode("Microsoft", "Business Administrator");
        mainGraph.createJobNode("Microsoft", "Senior Developer");
        mainGraph.createJobNode("Microsoft", "Systems Analyst");

        mainGraph.createJobNode("Amazon", "Business Administrator");
        mainGraph.createJobNode("Amazon", "Systems Analyst");
        mainGraph.createJobNode("Amazon", "Software Engineer");
        mainGraph.createJobNode("Amazon", "IT");
        mainGraph.createJobNode("Amazon", "Database Manager");
        mainGraph.createJobNode("Amazon", "Warehouse Associate");
        mainGraph.createJobNode("Amazon", "Warehouse Manager");

        mainGraph.createJobNode("Texas Instruments", "Computer Engineer");

        mainGraph.createJobNode("KPMG", "IT Project Manager");

        mainGraph.createJobNode("Equifax", "Information Security Officer");
        mainGraph.createJobNode("Equifax", "Systems Analyst");
        mainGraph.createJobNode("Equifax", "Business Analyst");

        mainGraph.createJobNode("Monsanto", "Data Analyst");
        mainGraph.createJobNode("Monsanto", "Chemical Engineer");
        mainGraph.createJobNode("Monsanto", "Botanist");
        mainGraph.createJobNode("Monsanto", "Weed Control Scientist");
        mainGraph.createJobNode("Monsanto", "Sales Representative");

        mainGraph.createJobNode("Dot Foods", "Database Manager");
        mainGraph.createJobNode("Dot Foods", "Warehouse Associate");
        mainGraph.createJobNode("Dot Foods", "Warehouse Manager");
        mainGraph.createJobNode("Dot Foods", "Business Analyst");

        mainGraph.createJobNode("Panera Bread", "Store Manager");
        mainGraph.createJobNode("Panera Bread", "Database Manager");
        mainGraph.createJobNode("Panera Bread", "Baker");
        mainGraph.createJobNode("Panera Bread", "Bakery Associate");
        mainGraph.createJobNode("Panera Bread", "Shift Supervisor");
        mainGraph.createJobNode("Panera Bread", "Warehouse Associate");
        mainGraph.createJobNode("Panera Bread", "Truck Driver");
        mainGraph.createJobNode("Panera Bread", "Sales Representative");

        mainGraph.createJobNode("Imo's", "Delivery Driver");
        mainGraph.createJobNode("Imo's", "Store Manager");
        mainGraph.createJobNode("Imo's", "Baker");
        mainGraph.createJobNode("Imo's", "Truck Driver");

        mainGraph.createJobNode("Starbucks", "Barista");
        mainGraph.createJobNode("Starbucks", "Shift Supervisor");
        mainGraph.createJobNode("Starbucks", "Store Manager");
        mainGraph.createJobNode("Starbucks", "Sales Representative");
        mainGraph.createJobNode("Starbucks", "Truck Driver");
        mainGraph.createJobNode("Starbucks", "Warehouse Manager");

        //0 is just a click
        //1 is the user liking the post
        //2 is the user disliking the post (I think this is a good idea so users can just get rid of jobs they don't like)
        mainGraph.clickOnJob("Starbucks", "Barista", 0);
        mainGraph.clickOnJob("Starbucks", "Shift Supervisor", 1);
        mainGraph.clickOnJob("Starbucks", "Store Manager", 1);

        mainGraph.clickOnJob("Equifax", "Systems Analyst", 1);
        mainGraph.clickOnJob("Equifax", "Business Analyst", 1);
        mainGraph.clickOnJob("Amazon", "Systems Analyst", 1);

        mainGraph.clickOnJob("Imo's", "Truck Driver", 2);

        mainGraph.clickOnJob("Monsanto", "Chemical Engineer", 2);



        /*{
            // START SNIPPET: findUsers
            Label label = Label.label( "Job" );
            String desiredJobTitle = "Systems Analyst";
            try ( Transaction tx = mainGraph.graphDb.beginTx() )
            {
                try ( ResourceIterator<Node> users =
                              mainGraph.graphDb.findNodes( label, "jobTitle", desiredJobTitle ) ) //This iterator is never closed?
                {
                    //This creates an arraylist of nodes with the jobID matching the desired jobNumber
                    //But really there is only going to be one node in there since jobID is a primary key
                    ArrayList<Node> userNodes = new ArrayList<>();
                    while ( users.hasNext() )
                    {
                        userNodes.add( users.next() );
                    }

                    for ( Node node : userNodes )
                    {
                        System.out.println(
                                "The company of the node with title " + desiredJobTitle + " is " + node.getProperty( "company" ) );
                    }
                }
            }//There's no tx.success()?
            // END SNIPPET: findUsers
        }*/

        /*{
            // START SNIPPET: resourceIterator
            //This part shows how to open an iterator to iterate through multiple nodes easier than an arraylist
            //This part doesn't actually do anything rn
            Label jobLabel = Label.label( "Job" );
            int desiredJobNumber = 4;
            String desiredJobID = "Job#" + desiredJobNumber + "@neo4j.org";
            try ( Transaction tx = mainGraph.graphDb.beginTx();
                  ResourceIterator<Node> users = mainGraph.graphDb.findNodes( jobLabel, "jobID", desiredJobID ) )
            {
                //Finds the first node with the right id
                Node firstUserNode;
                if ( users.hasNext() )
                {
                    firstUserNode = users.next();
                }
                users.close(); //Remember to close these things...?
            }
            // END SNIPPET: resourceIterator
        }*/

        {
            // START SNIPPET: updateUsers
            //This is how you find and update a node according to the neo4j boys but it's not working
            //Might be a problem with the static vs instance methods, because it used to work
            try ( Transaction tx = mainGraph.graphDb.beginTx() )
            {
                Label jobLabel = Label.label( "Job" );
                String desiredCompany = "Google";

                for ( Node node : loop( mainGraph.graphDb.findNodes( jobLabel, "company" , desiredCompany ) ) )
                {
                    node.setProperty( "company", "Alphabet" );
                }
                tx.success();
            }
            // END SNIPPET: updateUsers
        }

        /*{
            // START SNIPPET: dropIndex
            try ( Transaction tx = mainGraph.graphDb.beginTx() )
            {
                Label label = Label.label( "Job" );
                for ( IndexDefinition indexDefinition : mainGraph.graphDb.schema()
                        .getIndexes( label ) )
                {
                    // There is only one index
                    System.out.println(indexDefinition);
                    indexDefinition.drop();
                }

                tx.success();
            }
            // END SNIPPET: dropIndex
        }*/

        mainGraph.printNodesInOrderOfWeight();

        mainGraph.deleteNodesAndRelationships();
        System.out.println( "Shutting down database ..." );
        mainGraph.graphDb.shutdown();
    }
}