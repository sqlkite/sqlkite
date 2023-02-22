**Don't needless uppercase SQL**

Projects have a variety of limits (or capabilities), such as `max_concurrency`. These settings are persisted in the super database and loaded into the main `Project` structure. 


Remember that for parameter binding, PostgreSQL uses $N  (e.g. $1, $2, ...) and sqlite uses ?N (e.g. ?1, ?2, ...)

To add new limits:

1. Add the new field to the `Limits` structure in `data/project.go`. Data structures in the data package are meant to be dependency free and act as a bridge between parts of the code (most notably for the "super" database)

2. If you need to insert some default values into existing projects, create a new migration in `super/pg/migrations`. This involves creating a new file. As a convention, name the function within this file liek the file. For example, if you create a new migration file named `0020_project_limit_xyz.go`, then your function could be called `Migrate_0020`. Look at other migrations for inspiration). Modify the `super/pg/migrations/migrations.go` to run your function. You should update the `sqlkite_projects.data` column, which is a JSON serialized version of `data.Project`.

3. Repeat step 4 but for sqlite (i.e. `super/sqlite/migrations`).

The remaining steps are all about tests.

4. Modify the `Project` test factory in the `init` function of `tests/factory.go`. Specify a default that won't get in the way of tests that don't have anything to do with your new limit.

5. Modify the test databases created in `tests/setup/main.go`. The "limited" project should have a low limit which will let you easily test the error condition when going over the limit.

6. Modify the `Test_GetProject_Success` in both `super/pg/pg_test.go` and `super/sqlite/sqlite_test.go`. This test is asserting that project data is correctly being selected from the database.

7. Modify `Test_CreateProject` and `Test_UpdateProject` in both `super/pg/pg_test.go` and `super/sqlite/sqlite_test.go`. These are testing the insert and update project functions - we want to make sure that your new field is properly persisted.
