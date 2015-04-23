open System
open System.Linq
open Microsoft.Azure.Documents;
open Microsoft.Azure.Documents.Client;
open Microsoft.Azure.Documents.Linq;

let endpoint = "<YOUR ENDPOINT URL>"
let authKey = "<YOUR AUTH KEY>"
let client =
    new DocumentClient(new Uri(endpoint), authKey)

// The type that we want to persist
type Lp = { id : string ; Name : string ; Artist : string ; LentTo : string }

// Get or create a named database
let getDatabase dbName = async {
    let existing = 
        client.CreateDatabaseQuery()
        |> Seq.where (fun d -> d.Id = dbName)
        |> Seq.toList

    match existing with
    | [] -> 
        let! db = 
            Async.AwaitTask 
            <| client.CreateDatabaseAsync(new Database(Id = dbName))
        return db.Resource
    | db :: _ -> return db
}

// Get or create a named collection within a named database
let getCollection dbName collectionName = async {
    let! db = getDatabase dbName
    let existing = 
        client.CreateDocumentCollectionQuery(db.SelfLink)
        |> Seq.where (fun dc -> dc.Id = collectionName)
        |> Seq.toList

    match existing with
    | [] ->
        let! collection = 
            Async.AwaitTask 
            <| client.CreateDocumentCollectionAsync(
                db.CollectionsLink, 
                new DocumentCollection(Id = collectionName)
            )
        return collection.Resource
    | collection :: _ -> return collection
}

// Get operations
let getChainedLinq name = async {
    printfn "Get chained LINQ %s" name
    let! collection = getCollection "LpLibrary" "Lps"
    let query = 
        Seq.toList
        <| client.CreateDocumentQuery<Lp>(collection.DocumentsLink).Where(fun lp -> lp.Name = name)

    match query with
    | [] -> return None
    | document :: _ -> return Some(document)
}

let getEmbeddedQuery name = async {
    printfn "Get embedded query %s" name
    let! collection = getCollection "LpLibrary" "Lps"
    let parameters = new SqlParameterCollection([| new SqlParameter("@Name", name) |])
    let spec = new SqlQuerySpec("SELECT * FROM Lps lps WHERE lps.Name = @Name", parameters)
    let query = Seq.toList <| client.CreateDocumentQuery<Lp>(collection.DocumentsLink, spec)

    match query with
    | [] -> return None
    | document :: _ -> return Some(document)
}

let getPipedSequence name = async {
    printfn "Get piped sequence %s" name
    let! collection = getCollection "LpLibrary" "Lps"
    let query = 
        client.CreateDocumentQuery<Lp>(collection.DocumentsLink)
        |> Seq.filter (fun lp -> lp.Name = name)
        |> Seq.toList 

    match query with
    | [] -> return None
    | document :: _ -> return Some(document)
}

let getQueryExpression name = async {
    printfn "Get query expression %s" name
    let! collection = getCollection "LpLibrary" "Lps"
    let query = Seq.toList <| query {
        for d in client.CreateDocumentQuery<Lp>(collection.DocumentsLink) do 
        where (d.Name = name)
        select d
    }
    
    match query with
    | [] -> return None
    | document :: _ -> return Some(document)
}

// Add an entry to collection
let set (entry : Lp) = async {
    printfn "Set %s" entry.Name 
    let! collection = getCollection "LpLibrary" "Lps"
    client.CreateDocumentAsync(collection.SelfLink, entry) 
    |> Async.AwaitTask 
    |> ignore
}

// Delete operations
let deleteQueryExpression id = async {
    printfn "Delete %s" id
    let! collection = getCollection "LpLibrary" "Lps"
    let delete = Seq.toList <| query {
        for d in client.CreateDocumentQuery(collection.DocumentsLink) do 
        where (d.Id = id)
        select d
    }

    match delete with
    | [] -> return ()
    | document :: _ -> 
        document.SelfLink 
        |> client.DeleteDocumentAsync 
        |> Async.AwaitTask 
        |> ignore
}

let deleteEmbeddedQuery name = async {
    printfn "Delete %s" name
    let! collection = getCollection "LpLibrary" "Lps"
    let parameters = new SqlParameterCollection([| new SqlParameter("@Name", name) |])
    let spec = new SqlQuerySpec("SELECT * FROM Lps lps WHERE lps.Name = @Name", parameters)
    
    let delete =
        Seq.toList <| 
        client.CreateDocumentQuery<Document>(collection.DocumentsLink, spec).AsEnumerable()
    
    match delete with
    | [] -> return ()
    | document :: _ -> 
        document.SelfLink 
        |> client.DeleteDocumentAsync 
        |> Async.AwaitTask 
        |> ignore
}

let print record =
    match record with
    | Some(lp) -> printfn "%s by %s is lent to %s" lp.Name lp.Artist lp.LentTo
    | _ -> printfn "LP not found"

[<EntryPoint>]
let main argv = 
    
    let beachy = { id = "beachy"; Name = "Pet Sounds"; Artist = "The Beach Boys"; LentTo = "Greg" }
    let groovy = { id = "groovy"; Name = "What's Going On"; Artist = "Marvin Gaye"; LentTo = "Julia" }
    let punky = { id = "punky"; Name = "London Calling"; Artist = "The Clash"; LentTo = null }

    // Let's add some records
    Async.RunSynchronously <| async {
        do! set beachy
        do! set groovy
        do! set punky
    }

    // And then query for one of them
    Async.RunSynchronously <| async {
        let! groovyFromDb1 = getChainedLinq "Pet Sounds"        
        print groovyFromDb1

        let! groovyFromDb2 = getEmbeddedQuery "Pet Sounds"        
        print groovyFromDb2

        let! groovyFromDb3 = getPipedSequence "Pet Sounds"
        print groovyFromDb3

        let! groovyFromDb4 = getQueryExpression "Pet Sounds"
        print groovyFromDb4
    }

    // Before deleting them all again
    Async.RunSynchronously <| async {
        do! deleteQueryExpression "beachy"
        do! deleteQueryExpression "groovy"
        do! deleteEmbeddedQuery "London Calling"
    }

    Console.ReadLine()

    0
