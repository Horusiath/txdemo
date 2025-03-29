namespace TxDemo

open System.Threading

type TxId = uint64

[<CLIMutable;NoEquality;NoComparison>]
type Value = {
  value: string
  start: TxId
  mutable finish: TxId
}

type TransactionStatus =
  | InProgress = 0
  | Aborted    = 1
  | Committed  = 2
    
type IsolationLevel =
  | ReadUncommitted = 0
  | ReadCommitted  = 1
  | RepeatableRead = 2
  | Snapshot       = 3
  | Serializable   = 4
  
[<CLIMutable;NoEquality;NoComparison>]
type Transaction = {
  isolation: IsolationLevel
  id: TxId
  mutable status: TransactionStatus
  
  // Used only by Repeatable Read and stricter.
  mutable inProgress: Set<TxId>
  
  // Used only by Snapshot Isolation and stricter.
  mutable readSet: Set<string>
  mutable writeSet: Set<string>
}
    

[<CLIMutable;NoEquality;NoComparison>]
type Database = {
  mutable defaultIsolation: IsolationLevel
  mutable store: Map<string, Value list>
  mutable transactions: Map<TxId, Transaction>
  mutable nextTransactionId: uint64
}

[<CLIMutable;NoEquality;NoComparison>]
type Conn = {
  db: Database
  mutable tx: Transaction option
}

type Cmd =
  | Begin
  | Abort
  | Commit
  | Get    of key:string
  | Delete of key:string
  | Set    of key:string * value:string  

[<RequireQualifiedAccess>]
module Database =
  
  let create () =
    { defaultIsolation = IsolationLevel.ReadCommitted
      store = Map.empty
      transactions = Map.empty
      // The `0` transaction id will be used to mean that
      // the id was not set. So all valid transaction ids
      // must start at 1.
      nextTransactionId = 0UL }

  let inProgress db =
    db.transactions
    |> Seq.choose (fun e -> if e.Value.status = TransactionStatus.InProgress then Some e.Key else None)
    |> Set.ofSeq
    
  let beginTransaction db =
    let txId = Interlocked.Increment(&db.nextTransactionId)
    let tx =
      { id = txId
        isolation = db.defaultIsolation
        status = TransactionStatus.InProgress
        inProgress = inProgress db
        readSet = Set.empty
        writeSet = Set.empty }
    db.transactions <- Map.add txId tx db.transactions
    tx
    
  let transactionStatus txId db = db.transactions[txId].status
    
  let private conflictOnCommitted conflictFn t1 db =
    Seq.exists (fun id ->
      match Map.tryFind id db.transactions with
      | Some t2 when t2.status = TransactionStatus.Committed ->
        conflictFn t1 t2
      | _ -> false)
    
  let hasConflict t1 db  fn =
    // First see if there is any conflict with transactions that
    // were in progress when this one started.
    conflictOnCommitted fn t1 db t1.inProgress ||
    // Then see if there is any conflict with transactions that
    // started and committed after this one started.
    conflictOnCommitted fn t1 db (seq { t1.id .. db.nextTransactionId })
    
  let intersects s1 s2 =
    not (Set.isEmpty (Set.intersect s1 s2))
    
  let rec finishTransaction tx status db =
    if status = TransactionStatus.Committed then
      match tx.isolation with
      | IsolationLevel.Snapshot when hasConflict tx db (fun t1 t2 -> intersects t1.writeSet t2.writeSet) ->
        // Snapshot Isolation imposes the additional constraint that no transaction A
        // may commit after writing any of the same keys as transaction B has written
        // and committed during transaction A's life.
        finishTransaction tx TransactionStatus.Aborted db
        failwith "write-write conflict"
      | IsolationLevel.Serializable when hasConflict tx db (fun t1 t2 -> intersects t1.readSet t2.writeSet || intersects t1.writeSet t2.readSet) ->
        finishTransaction tx TransactionStatus.Aborted db
        failwith "read-write conflict"
      | _ -> ()
    tx.status <- status
    db.transactions <- Map.add tx.id tx db.transactions
    
  let isVisible db tx value =
    match tx.isolation with
    | IsolationLevel.ReadUncommitted -> 
      // Read Uncommitted means we simply read the last value
      // written. Even if the transaction that wrote this value has
      // not committed, and even if it has aborted.
      value.finish = 0UL // We must merely make sure the value has not been deleted
      
    | IsolationLevel.ReadCommitted ->
      // Read Committed means we are allowed to read any values that
      // are committed at the point in time where we read.
      if value.start <> tx.id && transactionStatus value.start db <> TransactionStatus.Committed then
        // If the value was created by a transaction that is
        // not committed, and not this current transaction, it's no good.
        false
      elif value.finish = tx.id then
        false // If the value was deleted in this transaction, it's no good.
      elif value.finish > 0UL && transactionStatus value.finish db = TransactionStatus.Committed then
        false // Or if the value was deleted in some other committed transaction, it's no good.
      else true
      
    | IsolationLevel.RepeatableRead | IsolationLevel.Snapshot | IsolationLevel.Serializable ->
      // Repeatable Read, Snapshot Isolation, and Serializable
      // further restricts Read Committed so only versions from
      // transactions that completed before this one started are visible.
      // Snapshot Isolation and Serializable will do additional checks at commit time.
      if value.start > tx.id then
        false // Ignore values from transactions started after this one.
      elif Set.contains value.start tx.inProgress then        
        false // Ignore values created from transactions in progress when this one started.
      elif transactionStatus value.start db <> TransactionStatus.Committed && value.start <> tx.id then
        // If the value was created by a transaction that is not committed,
        // and not this current transaction, it's no good.
        false
      elif value.finish = tx.id then
        false // If the value was deleted in this transaction, it's no good.
      elif value.finish < tx.id && value.finish > 0UL
           && transactionStatus value.finish db = TransactionStatus.Committed
           && not(Set.contains value.finish tx.inProgress) then
        // Or if the value was deleted in some other committed
        // transaction that started before this one, it's no good.
        false
      else true

    | _ -> failwith "not implemented"
  
    
  let connect db =
    { tx = None; db = db  }

[<RequireQualifiedAccess>]
module Conn =
  
  // Mark all visible versions as now invalid.
  let private invalidate conn key =
    let tx = conn.tx.Value
    let mutable found = false
    match Map.tryFind key conn.db.store with
    | None -> false
    | Some values ->
      for value in values do
        if Database.isVisible conn.db tx value then
          value.finish <- tx.id
          found <- true
      found
    
  
  let exec cmd conn : string option =
    match cmd with
    | Cmd.Begin ->
      let tx = Database.beginTransaction conn.db
      conn.tx <- Some tx
      None
    | Cmd.Abort ->
      Database.finishTransaction conn.tx.Value TransactionStatus.Aborted conn.db
      conn.tx <- None
      None
    | Cmd.Commit ->
      Database.finishTransaction conn.tx.Value TransactionStatus.Committed conn.db
      conn.tx <- None
      None
    | Cmd.Get key ->
      let tx = conn.tx.Value
      tx.readSet <- Set.add key tx.readSet
      Map.tryFind key conn.db.store
      |> Option.bind (List.tryFind (Database.isVisible conn.db tx))
      |> Option.map _.value
    | Cmd.Delete key ->
      if invalidate conn key then
        let tx = conn.tx.Value
        tx.writeSet <- Set.add key tx.writeSet
      None
    | Cmd.Set (key, value) ->
      invalidate conn key |> ignore
      let tx = conn.tx.Value
      tx.writeSet <- Set.add key tx.writeSet
      // And add a new version if it's a set command.
      let e = { start = tx.id; finish = 0UL; value = value }
      conn.db.store <- conn.db.store |> Map.change key (function Some vals -> Some (e::vals) | None -> Some [e])
      None