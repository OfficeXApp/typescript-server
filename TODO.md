# TODO

- [x] Vibe-migrate core logic
- [x] Implement actual group handling logic based on permissions/ownership.
- [x] Implement actual permission handling logic based on permissions/ownership.
- [x] Implement actual webhook handling logic based on permissions/ownership.
- [x] Fix breadcrumbs on directory
- [x] Audit folders and subfolder creation, right now its buggy. (check if UPDATE_FOLDER had same bug as UPDATE_FILE before - might need to implement patch on UPDATE_FOLDER too, use UPDATE_FILE as reference dbHelpers.runDrive)
- [x] Fix bug where files/folders cannot be renamed without clicking into it (similar with clicking dot menu, it just enters the resource)
- [x] Audit file sharing flows
- [x] Audit group invite flows
- [x] Audit permit sharing flows (for example, right now anyone in a group can edit group)

- [x] Fix recents & trashbin
- [x] Fix file/folder copy move
- [x] Add rate-limiting to web2 server
- [x] Disable creating new database on rest call at unknown org route (should throw 404. db should only be created via giftcard spawnorg). sometimes a drive has been deleted, so the db no longer exists. potentailly need to incorporate into error message.
- [x] Default allow "Group for All" access to View all disks

- [ ] Fix multi-tab organizations, removing localstorage dependencies

- [ ] Search Drive
- [ ] Fix superswap, old users not removed from frontend
- [ ] Fix bug where password access to a folder doesnt work (but file does)
- [ ] Fix bug where cannot update a spreadsheet without access to parent folder create permit

- [ ] Audit storj upload flows
- [ ] Implement UUID tracking and usage
- [ ] Implement state diffs logic

- [ ] Implement actual label handling logic based on permissions/ownership.
- [ ] Consider offering raw POST url uploading (ie. the web2 version of canister uploading)

- [ ] Fix copy, cannot actually copy in S3/AWS
- [ ] Fix move/copy navigation on frontend (its flat, not tree)
