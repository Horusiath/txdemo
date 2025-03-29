module Tests

open Xunit
open TxDemo

[<Fact>]
let ``Read Uncommitted`` () =
  let db = Database.create ()
  db.defaultIsolation <- IsolationLevel.ReadUncommitted
  
  let c1 = Database.connect db
  Conn.exec Cmd.Begin c1 |> ignore
  
  let c2 = Database.connect db
  Conn.exec Cmd.Begin c2 |> ignore
  
  Conn.exec (Cmd.Set("x", "hey")) c1 |> ignore
  // Update is visible to self.
  assert (Some "hey" = Conn.exec (Cmd.Get "x") c1)
  
  // But since read uncommitted, also available to everyone else.
  assert (Some "hey" = Conn.exec (Cmd.Get "x") c2)
  
  // And if we delete, that should be respected.
  Conn.exec (Cmd.Delete "x") c1 |> ignore
  assert (None = Conn.exec (Cmd.Get "x") c1)
  assert (None = Conn.exec (Cmd.Get "x") c2)
  
[<Fact>]
let ``Read Committed`` () =
  let db = Database.create ()
  db.defaultIsolation <- IsolationLevel.ReadCommitted
  
  let c1 = Database.connect db
  Conn.exec Cmd.Begin c1 |> ignore
  
  let c2 = Database.connect db
  Conn.exec Cmd.Begin c2 |> ignore
  
  // Local change is visible locally.
  Conn.exec (Cmd.Set("x", "hey")) c1 |> ignore
  assert (Some "hey" = Conn.exec (Cmd.Get "x") c1)
  
  // Update not available to this transaction since this is not committed
  assert (None = Conn.exec (Cmd.Get "x") c2)
  
  Conn.exec Cmd.Commit c1 |> ignore
  // Now that it's been committed, it's visible in c2.
  assert (Some "hey" = Conn.exec (Cmd.Get "x") c2)
  
  let c3 = Database.connect db
  Conn.exec Cmd.Begin c3 |> ignore
  // Local change is visible locally.
  Conn.exec (Cmd.Set("x", "yall")) c3 |> ignore
  assert (Some "yall" = Conn.exec (Cmd.Get "x") c3)
  
  // But not on the other commit, again.
  assert (Some "hey" = Conn.exec (Cmd.Get "x") c2)
  
  Conn.exec Cmd.Abort c3 |> ignore
  // And still not, if the other transaction aborted.
  assert (Some "hey" = Conn.exec (Cmd.Get "x") c2)
  
  // And if we delete it, it should show up deleted locally.
  Conn.exec (Cmd.Delete "x") c2 |> ignore
  assert (None = Conn.exec (Cmd.Get "x") c2)
  Conn.exec Cmd.Commit c2 |> ignore
  
  // It should also show up as deleted in new transactions now
  // that it has been committed.
  let c4 = Database.connect db
  Conn.exec Cmd.Begin c4 |> ignore
  assert (None = Conn.exec (Cmd.Get "x") c4)
  
[<Fact>]
let ``Repeatable Read`` () =
  let db = Database.create ()
  db.defaultIsolation <- IsolationLevel.RepeatableRead
  
  let c1 = Database.connect db
  Conn.exec Cmd.Begin c1 |> ignore
  
  let c2 = Database.connect db
  Conn.exec Cmd.Begin c2 |> ignore
  
  // Local change is visible locally.
  Conn.exec (Cmd.Set("x", "hey")) c1 |> ignore
  assert (Some "hey" = Conn.exec (Cmd.Get "x") c1)
  
  // Update not available to this transaction since this is not committed.
  assert (None = Conn.exec (Cmd.Get "x") c2)
  
  Conn.exec Cmd.Commit c1 |> ignore
  // Even after committing, it's not visible in an existing transaction.
  assert (None = Conn.exec (Cmd.Get "x") c2)
  
  // But is available in a new transaction.
  let c3 = Database.connect db
  Conn.exec Cmd.Begin c3 |> ignore
  assert (Some "hey" = Conn.exec (Cmd.Get "x") c3)
  
  // Local change is visible locally.
  Conn.exec (Cmd.Set("x", "yall")) c3 |> ignore
  assert (Some "yall" = Conn.exec (Cmd.Get "x") c3)
  
  // But not on the other commit, again.
  assert (None = Conn.exec (Cmd.Get "x") c2)
  
  Conn.exec Cmd.Abort c3 |> ignore
  // And still not, regardless of abort, because it's an older transaction.
  assert (None = Conn.exec (Cmd.Get "x") c2)
  
  // And again still the aborted set is still not on a new transaction.
  let c4 = Database.connect db
  Conn.exec Cmd.Begin c4 |> ignore
  assert (Some "hey" = Conn.exec (Cmd.Get "x") c4)
  
  Conn.exec (Cmd.Delete "x") c4 |> ignore
  Conn.exec Cmd.Commit c4 |> ignore
  // But the delete is visible to new transactions now that this has been committed.
  let c5 = Database.connect db
  Conn.exec Cmd.Begin c5 |> ignore
  assert (None = Conn.exec (Cmd.Get "x") c5)
  
[<Fact>]
let ``Snapshot write/write conflict`` () =
  let db = Database.create ()
  db.defaultIsolation <- IsolationLevel.Snapshot
  
  let c1 = Database.connect db
  Conn.exec Cmd.Begin c1 |> ignore
  
  let c2 = Database.connect db
  Conn.exec Cmd.Begin c2 |> ignore
  
  let c3 = Database.connect db
  Conn.exec Cmd.Begin c3 |> ignore
  
  Conn.exec (Cmd.Set("x", "hey")) c1 |> ignore
  Conn.exec Cmd.Commit c1 |> ignore
  
  Conn.exec (Cmd.Set("x", "hey")) c2 |> ignore
  
  // expecting failure on concurrent commit for the same values
  assert (try Conn.exec Cmd.Commit c2 |> ignore; false with e -> e.Message = "write-write conflict")
  
  // But unrelated keys cause no conflict.
  Conn.exec (Cmd.Set("y", "no conflict")) c3 |> ignore
  Conn.exec Cmd.Commit c3 |> ignore
  
[<Fact>]
let ``Serializable read/write conflict`` () =
  let db = Database.create ()
  db.defaultIsolation <- IsolationLevel.Serializable
  
  let c1 = Database.connect db
  Conn.exec Cmd.Begin c1 |> ignore
  
  let c2 = Database.connect db
  Conn.exec Cmd.Begin c2 |> ignore
  
  let c3 = Database.connect db
  Conn.exec Cmd.Begin c3 |> ignore
  
  Conn.exec (Cmd.Set("x", "hey")) c1 |> ignore
  Conn.exec Cmd.Commit c1 |> ignore
  
  assert (None = Conn.exec (Cmd.Get "x") c2) // cannot get key that does not exist
  assert (try Conn.exec Cmd.Commit c2 |> ignore; false with e -> e.Message = "read-write conflict")
  
  // But unrelated keys cause no conflict.
  Conn.exec (Cmd.Set("y", "no conflict")) c3 |> ignore
  Conn.exec Cmd.Commit c3 |> ignore
  
  