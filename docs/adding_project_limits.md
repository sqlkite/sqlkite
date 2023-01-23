**Don't needless uppercase SQL**

Projects have a variety of limits (or capabilities), such as `max_concurrency`. These settings are persisted in the super database and loaded into the main `Project` structure. 

Yes, some of this would be unnecessary if we simply defined a default database value for these limits. But I can't bring myself to doing this. I really feel like these values really are really decided on an install-by-install basis and not baked into the data definition.

Remember that for parameter binding, PostgreSQL uses $N  (e.g. $1, $2, ...) and sqlite uses ?N (e.g. ?1, ?2, ...)

To add new limits and unfortunately large number of changes must be made:

1. Add the new field to the `Project` structure in `project.go`. E.g. `MaxTableCount uint16`

2. Add this same field to the `Project` structure in `data/project.go`. These data structures are meant to be dependency-free and mainly bridge database data with the rest of the code.

3. In `project.go`, within the `NewProject` function, populate the `Project` field from the `data.Project` object (look for where the `Project` is created, i.e. `&Project{...}`)

4. Create a new migration in `super/pg/migrations`. This involves creating a new file. As a convention, name the function within this file liek the file. For example, if you create a new migration file named `0020_project_limit_xyz.go`, then your function could be called `Migrate_0020`. Look at other migrations for inspiration). Modify the `super/pg/migrations/migrations.go` to run your function.

5. Repeat step 4 but for sqlite (i.e. `super/sqlite/migrations`).

6. Modify `super/pg/pg.go`. Select the new column in the `GetProject`, `GetUpdatedProjects`, `CreateProject` and `UpdateProject` functions. The latter two functions require that you pass the new field to bind to the SQL statement. Also add the field the to `scanProject` function which is used to bind the result of a `select * from project [where...]` query into a `data.Project` structure.

7. Repeat step 6 but for sqlite (i.e. `super/sqlite/sqlite.go`).


The remaining steps are all about tests.

8. Modify the `Project` test factory in the `init` function of `tests/factory.go`. Specify a default that won't get in the way of tests that don't have anything to do with your new limit.

9. Modify the test databases created in `tests/setup/main.go`. The "limited" project should have a low limit which will let you easily test the error condition when going over the limit.

10. Modify the `Test_GetProject_Success` in both `super/pg/pg_test.go` and `super/sqlite/sqlite_test.go`. This test is asserting that project data is correctly being selected from the database.

11. Modify `Test_GetUpdatedProjects_None` and `Test_GetUpdatedProjects_Success` in both `super/pg/pg_test.go` and `super/sqlite/sqlite_test.go`. For both tests, the value that you insert into the database doesn't matter (you'll see integers are all `0`).

12. Modify `Test_CreateProject` and `Test_UpdateProject` in both `super/pg/pg_test.go` and `super/sqlite/sqlite_test.go`. These are testing the insert and update project functions - we want to make sure that your new field is properly persisted.
