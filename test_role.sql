-- Test CREATE ROLE / DROP ROLE / GRANT role TO user / REVOKE role FROM user

CREATE ROLE testrole;
CREATE ROLE readonly;

-- Test duplicate role
CREATE ROLE testrole;

CREATE USER bob bob user;

GRANT testrole TO bob;
GRANT readonly TO bob;

-- Test duplicate grant
GRANT testrole TO bob;

-- Test grant non-existent role
GRANT nonexist TO bob;

-- Test checkAdmin via role: create a user with no admin, grant admin role
CREATE USER charlie charlie user;
GRANT admin TO charlie;

-- Revoke role
REVOKE readonly FROM bob;

-- Test revoke non-granted role
REVOKE readonly FROM bob;

-- Drop role
DROP ROLE testrole;

-- Test drop non-existent role
DROP ROLE testrole;

-- Show roles for bob (via SHOW GRANTS or similar)
SHOW GRANTS bob;

DROP USER bob;
DROP USER charlie;
DROP ROLE readonly;
