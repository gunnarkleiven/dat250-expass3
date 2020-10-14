import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;

import org.bson.Document;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;


public class Main {

    public static void main(String[] args) {
        try {
            // Connect to MongoDB Server on localhost, port 27017 (default)
            final MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
            // Connect to Database "testDataBase"
            final MongoDatabase database = mongoClient.getDatabase("testDataBase");
            System.out.println("Successful database connection established. \n");


            // Insert document into database
            MongoCollection collection = database.getCollection("items");
            //insertManyDocuments(collection);

            // Query the documents in the database
            //queryDocuments(collection);


            // Update documents
            //updateDocuments(collection);

            // Delete documents
            deleteDocuments(collection);



            // Print all Documents
            FindIterable<Document> findIterable = collection.find(new Document());
            for (Document doc : findIterable) {
                System.out.println(doc);
            }



        } catch (Exception exception) {
            System.err.println(exception.getClass().getName() + ": " + exception.getMessage());
        }



    }

    private static void queryDocuments(MongoCollection collection) {
        // Query Documents in the database
        collection.insertMany(asList(
                Document.parse("{ item: 'journal', qty: 25, size: { h: 14, w: 21, uom: 'cm' }, status: 'A' }"),
                Document.parse("{ item: 'notebook', qty: 50, size: { h: 8.5, w: 11, uom: 'in' }, status: 'A' }"),
                Document.parse("{ item: 'paper', qty: 100, size: { h: 8.5, w: 11, uom: 'in' }, status: 'D' }"),
                Document.parse("{ item: 'planner', qty: 75, size: { h: 22.85, w: 30, uom: 'cm' }, status: 'D' }"),
                Document.parse("{ item: 'postcard', qty: 45, size: { h: 10, w: 15.25, uom: 'cm' }, status: 'A' }")
        ));

        System.out.println("------------------------------------------------------------------------------------");
        System.out.println("SQL QUERY: SELECT * FROM items WHERE status = \"D\"");
        // This corresponds to the SQL query "SELECT * FROM inventory WHERE status = "D""
        FindIterable<Document>  findIterable = collection.find(eq("status", "D"));
        for (Document doc : findIterable) {
            System.out.println(doc);
        }

        System.out.println("------------------------------------------------------------------------------------");
        System.out.println("SELECT * FROM items WHERE status in (\"A\", \"D\")");
        findIterable = collection.find(in("status", "A", "D"));
        for (Document doc : findIterable) System.out.println(doc);

        System.out.println("------------------------------------------------------------------------------------");
        System.out.println("SELECT * FROM inventory WHERE status = \"A\" AND ( qty < 30 OR item LIKE \"p%\")");
        findIterable = collection.find(
                and(eq("status", "A"),
                        or(lt("qty", 30), regex("item", "^p")))
        );
        for (Document doc : findIterable) System.out.println(doc);
    }

    private static void deleteDocuments(MongoCollection collection) {
        // Deletes the first document where status is "D"
        collection.deleteOne(eq("status", "D"));

        collection.deleteMany(eq("status", "A"));


    }

    private static void updateDocuments(MongoCollection collection) {
        // Update one Document
        collection.updateOne(eq("item", "paper"),
                combine(set("size.uom", "cm"), set("status", "P"), currentDate("lastModified")));
        // Update several Documents
        collection.updateMany(lt("qty", 50),
                combine(set("size.uom", "in"), set("status", "P"), currentDate("lastModified")));

        // Replace a Document
        collection.replaceOne(eq("item", "paper"),
                Document.parse("{ item: 'paper', instock: [ { warehouse: 'A', qty: 60 }, { warehouse: 'B', qty: 40 } ] }"));
    }

    private static void insertManyDocuments(MongoCollection collection) {
        Document canvas = new Document("item", "canvas")
                .append("qty", 100)
                .append("tags", singletonList("cotton"));

        Document size = new Document("h", 28)
                .append("w", 35.5)
                .append("uom", "cm");
        canvas.put("size", size);

        collection.insertOne(canvas);

        FindIterable<Document> findIterable = collection.find(eq("item", "canvas"));


        // Insert multiple documents
        Document journal = new Document("item", "journal")
                .append("qty", 25)
                .append("tags", asList("blank", "red"));

        Document journalSize = new Document("h", 14)
                .append("w", 21)
                .append("uom", "cm");
        journal.put("size", journalSize);

        Document mat = new Document("item", "mat")
                .append("qty", 85)
                .append("tags", singletonList("gray"));

        Document matSize = new Document("h", 27.9)
                .append("w", 35.5)
                .append("uom", "cm");
        mat.put("size", matSize);

        Document mousePad = new Document("item", "mousePad")
                .append("qty", 25)
                .append("tags", asList("gel", "blue"));

        Document mousePadSize = new Document("h", 19)
                .append("w", 22.85)
                .append("uom", "cm");
        mousePad.put("size", mousePadSize);


        collection.insertMany(asList(journal, mat, mousePad));



    }
}
