import {
  DirectoryPermission,
  DirectoryPermissionFE,
  DirectoryPermissionType,
  IDPrefixEnum,
  DirectoryResourceID,
  UserID,
  GranteeID,
  FolderID,
  FileID,
  SystemPermissionType,
  SystemTableValueEnum,
  BreadcrumbVisibilityPreviewEnum,
  DriveID,
  SortDirection,
  PermissionMetadata,
  PermissionMetadataTypeEnum,
  PermissionMetadataContent,
  DriveClippedFilePath,
  LabelValue,
  GroupID,
  FilePathBreadcrumb,
  IRequestGetDirectoryPermission,
  IResponseGetDirectoryPermission,
  IRequestListDirectoryPermissions,
  IResponseListDirectoryPermissions,
  IRequestCreateDirectoryPermission,
  IResponseCreateDirectoryPermission,
  IRequestUpdateDirectoryPermission,
  IResponseUpdateDirectoryPermission,
  IRequestDeleteDirectoryPermission,
  IResponseDeleteDirectoryPermission,
  IRequestCheckDirectoryPermissions,
  IResponseCheckDirectoryPermissions,
  IRequestRedeemDirectoryPermission,
  IResponseRedeemDirectoryPermission,
  IRequestGetSystemPermission,
  IResponseGetSystemPermission,
  IRequestListSystemPermissions,
  IResponseListSystemPermissions,
  IRequestCreateSystemPermission,
  IResponseCreateSystemPermission,
  IRequestUpdateSystemPermission,
  IResponseUpdateSystemPermission,
  IRequestDeleteSystemPermission,
  IResponseDeleteSystemPermission,
  IRequestCheckSystemPermissions,
  IResponseCheckSystemPermissions,
  IRequestRedeemSystemPermission,
  IResponseRedeemSystemPermission,
  SystemPermission,
  SystemPermissionID,
  GenerateID,
  SystemPermissionFE,
  SystemResourceID,
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import { v4 as uuidv4 } from "uuid";
import { FastifyRequest, FastifyReply } from "fastify";
import {
  canUserAccessDirectoryPermission,
  castToDirectoryPermissionFE,
  checkDirectoryPermissions,
  deriveBreadcrumbVisibilityPreviews,
  mapDbRowToDirectoryPermission,
} from "../../../../services/permissions/directory";
import { createApiResponse, getDriveOwnerId } from "../../types";
import {
  getFileMetadata,
  getFolderMetadata,
} from "../../../../services/directory/drive";
import {
  castIdToTable,
  castToSystemPermissionFE,
  checkSystemPermissions,
  mapDbRowToSystemPermission,
} from "../../../../services/permissions/system";
import { authenticateRequest } from "../../../../services/auth";

// Constants for ID prefixes to match Rust enum variants in string format
export const PUBLIC_GRANTEE_ID_STRING = "PUBLIC";
export const USER_ID_PREFIX = IDPrefixEnum.User;
export const GROUP_ID_PREFIX = IDPrefixEnum.Group;
export const PLACEHOLDER_GRANTEE_ID_PREFIX =
  IDPrefixEnum.PlaceholderPermissionGrantee;

// --- Helper Functions for Internal Mapping ---

/**
 * Parses a GranteeID string (e.g., "USER_abc", "GROUP_xyz", "PUBLIC")
 * into its type and raw ID part for database storage.
 * @param idStr The GranteeID string.
 * @returns { type: string, id: string | null } - The grantee_type and grantee_id for DB.
 */
function parseGranteeIDForDb(idStr: GranteeID): {
  type: string;
  id: string | null;
} {
  console.log(`>>> parseGranteeIDForDb: ${idStr}`);
  if (idStr === PUBLIC_GRANTEE_ID_STRING) {
    return { type: "Public", id: null };
  }
  if (idStr.startsWith(USER_ID_PREFIX)) {
    return { type: "User", id: idStr };
  }
  if (idStr.startsWith(GROUP_ID_PREFIX)) {
    return { type: "Group", id: idStr };
  }
  if (idStr.startsWith(PLACEHOLDER_GRANTEE_ID_PREFIX)) {
    return {
      type: "Placeholder",
      id: idStr,
    };
  }
  throw new Error(`Invalid GranteeID format for DB: ${idStr}`);
}

/**
 * Parses a DirectoryResourceID string (e.g., "FileID_abc", "FolderID_xyz")
 * into its type and raw ID part for database storage.
 * @param idStr The DirectoryResourceID string.
 * @returns { type: string, id: string } - The resource_type and resource_id for DB.
 */
function parseDirectoryResourceIDForDb(idStr: DirectoryResourceID): {
  type: string;
  id: string;
} {
  if (idStr.startsWith(IDPrefixEnum.File)) {
    return { type: "File", id: idStr };
  }
  if (idStr.startsWith(IDPrefixEnum.Folder)) {
    return { type: "Folder", id: idStr };
  }
  throw new Error(`Invalid DirectoryResourceID format for DB: ${idStr}`);
}

async function isUserInGroup(
  userId: UserID,
  groupId: GroupID,
  orgId: string
): Promise<boolean> {
  const rows = await db.queryDrive(
    orgId,
    "SELECT 1 FROM group_invites WHERE invitee_id = ? AND group_id = ?",
    [userId, groupId]
  );
  return rows.length > 0;
}

// --- Directory Permission Core Logic ---

/**
 * Retrieves a directory permission by its ID.
 * @param orgId The drive ID.
 * @param permissionId The ID of the permission to retrieve.
 * @param requesterId The ID of the user requesting the permission (for internal auth/previews).
 * @param skipAuthorizationCheck If true, returns the raw permission without checking if `requesterId` can access _this record_.
 * @returns The raw DirectoryPermission object, or null if not found.
 */
export async function getDirectoryPermissionById(
  orgId: string,
  permissionId: string,
  requesterId: UserID,
  skipAuthorizationCheck: boolean = false
): Promise<DirectoryPermission | null> {
  const query = `
    SELECT
        pd.id, pd.resource_type, pd.resource_id, pd.resource_path,
        pd.grantee_type, pd.grantee_id, pd.granted_by,
        GROUP_CONCAT(pdt.permission_type) AS permission_types_list,
        pd.begin_date_ms, pd.expiry_date_ms, pd.inheritable, pd.note,
        pd.created_at, pd.last_modified_at, pd.redeem_code, pd.from_placeholder_grantee,
        pd.metadata_type, pd.metadata_content, pd.external_id, pd.external_payload,
        GROUP_CONCAT(pdl.label_id) AS labels_list
    FROM permissions_directory pd
    LEFT JOIN permissions_directory_types pdt ON pd.id = pdt.permission_id
    LEFT JOIN permission_directory_labels pdl ON pd.id = pdl.permission_id
    WHERE pd.id = ?
    GROUP BY pd.id;`;

  const rows = await db.queryDrive(orgId, query, [permissionId]);

  if (rows.length === 0) {
    return null;
  }

  const rawPermission = mapDbRowToDirectoryPermission(rows[0]);

  if (!skipAuthorizationCheck) {
    const canAccess = await canUserAccessDirectoryPermission(
      rawPermission.resource_id,
      requesterId,
      orgId
    );
    if (!canAccess) {
      return null;
    }
  }

  return rawPermission;
}

/**
 * Lists directory permissions for a specific resource, with pagination and authorization.
 * @param options Query options including resourceId, requesterId, pageSize, direction, and cursor.
 * @returns An object containing items, total count, new cursor, and authorization status.
 */
export async function listDirectoryPermissionsForResource(options: {
  orgId: string;
  resourceId: DirectoryResourceID;
  requesterId: UserID;
  pageSize: number;
  direction: SortDirection;
  cursor?: string | null;
}): Promise<{
  items: DirectoryPermissionFE[];
  total: number;
  newCursor?: string | null;
  authorized: boolean;
}> {
  const { orgId, resourceId, requesterId, pageSize, direction, cursor } =
    options;
  const items: DirectoryPermissionFE[] = [];
  let total = 0;
  let newCursor: string | null = null;

  const isOwner = (await getDriveOwnerId(orgId)) === requesterId;

  const resourcePermissions = await checkDirectoryPermissions(
    resourceId,
    requesterId,
    orgId
  );

  console.log(
    `Requester ID: ${requesterId} on orgId: ${orgId} on resourceId: ${resourceId} with owner ${await getDriveOwnerId(orgId)} and directory permissions ${resourcePermissions}`
  );

  const hasViewPermissionOnResource = resourcePermissions.includes(
    DirectoryPermissionType.VIEW
  );

  if (!isOwner && !hasViewPermissionOnResource) {
    return { items: [], total: 0, authorized: false };
  }

  console.log(`>>> we continue here`);

  let query = `
    SELECT
        pd.id, pd.resource_type, pd.resource_id, pd.resource_path,
        pd.grantee_type, pd.grantee_id, pd.granted_by,
        GROUP_CONCAT(DISTINCT pdt.permission_type) AS permission_types_list,
        pd.begin_date_ms, pd.expiry_date_ms, pd.inheritable, pd.note,
        pd.created_at, pd.last_modified_at, pd.redeem_code, pd.from_placeholder_grantee,
        pd.metadata_type, pd.metadata_content, pd.external_id, pd.external_payload,
        GROUP_CONCAT(DISTINCT pdl.label_id) AS labels_list
    FROM permissions_directory pd
    LEFT JOIN permissions_directory_types pdt ON pd.id = pdt.permission_id
    LEFT JOIN permission_directory_labels pdl ON pd.id = pdl.permission_id
    WHERE pd.resource_id = ?
    GROUP BY pd.id
  `;

  const countQuery = `SELECT COUNT(DISTINCT id) AS count FROM permissions_directory WHERE resource_id = ?`;
  const countRows = await db.queryDrive(orgId, countQuery, [resourceId]);
  total = countRows[0]?.count || 0;

  let params: any[] = [resourceId];
  let orderByClause = `ORDER BY pd.created_at ${direction}`;
  let limitClause = `LIMIT ?`;

  if (cursor) {
    const cursorTime = parseInt(cursor, 10);
    if (direction === SortDirection.ASC) {
      orderByClause = `ORDER BY pd.created_at ASC`;
      query += ` AND pd.created_at > ?`;
      params.push(cursorTime);
    } else {
      orderByClause = `ORDER BY pd.created_at DESC`;
      query += ` AND pd.created_at < ?`;
      params.push(cursorTime);
    }
  }

  query += ` ${orderByClause} ${limitClause}`;
  params.push(pageSize + 1);

  const rows = await db.queryDrive(orgId, query, params);

  console.log(`>>> we got rows`, rows);

  for (let i = 0; i < rows.length && i < pageSize; i++) {
    const rawPerm = mapDbRowToDirectoryPermission(rows[i]);
    const canAccessRecord = await canUserAccessDirectoryPermission(
      rawPerm.resource_id,
      requesterId,
      orgId
    );
    if (canAccessRecord) {
      items.push(
        await castToDirectoryPermissionFE(rawPerm, requesterId, orgId)
      );
    }
  }

  if (rows.length > pageSize) {
    newCursor = rows[pageSize - 1].created_at.toString();
  }

  return { items, total, newCursor, authorized: true };
}

/**
 * Creates a new directory permission.
 * @param orgId The drive ID.
 * @param data The request body containing permission details.
 * @param requesterId The user creating the permission.
 * @returns The created DirectoryPermissionFE.
 */
export async function createDirectoryPermission(
  orgId: string,
  data: {
    id?: DirectoryPermission["id"];
    resource_id: DirectoryResourceID;
    granted_to?: GranteeID;
    permission_types: DirectoryPermissionType[];
    begin_date_ms?: number;
    expiry_date_ms?: number;
    inheritable: boolean;
    note?: string;
    metadata?: PermissionMetadata;
    external_id?: string;
    external_payload?: string;
  },
  requesterId: UserID
): Promise<DirectoryPermissionFE> {
  const newPermissionId =
    data.id || IDPrefixEnum.DirectoryPermission + uuidv4();
  const currentTime = Date.now();

  const granteeIdForDb =
    data.granted_to || GenerateID.PlaceholderPermissionGrantee();

  const { type: resourceType, id: resourceUUID } =
    parseDirectoryResourceIDForDb(data.resource_id);
  const { type: granteeType, id: granteeUUID } =
    parseGranteeIDForDb(granteeIdForDb);

  console.log(`
    >>> 

    resourceType: ${resourceType}
    resourceUUID: ${resourceUUID}
    granteeType: ${granteeType}
    granteeUUID: ${granteeUUID}
    
    `);

  let resourcePath = "";
  if (resourceType === "File") {
    const file = await getFileMetadata(orgId, resourceUUID as FileID);
    if (file) resourcePath = file.full_directory_path;
  } else if (resourceType === "Folder") {
    const folder = await getFolderMetadata(orgId, resourceUUID as FolderID);
    if (folder) resourcePath = folder.full_directory_path;
  }
  if (!resourcePath) {
    throw new Error(
      `Resource with ID ${data.resource_id} not found to determine its path.`
    );
  }

  const metadataType = data.metadata?.metadata_type || null;
  let metadataContent: string | null = null;
  if (data.metadata) {
    if ("Labels" in data.metadata.content) {
      metadataContent = data.metadata.content.Labels;
    } else if ("DirectoryPassword" in data.metadata.content) {
      metadataContent = data.metadata.content.DirectoryPassword;
    }
  }

  const redeem_code =
    !data.granted_to && granteeIdForDb.startsWith(PLACEHOLDER_GRANTEE_ID_PREFIX)
      ? GenerateID.RedeemCode()
      : null;

  // 1. Run the synchronous transaction to write the data to the database.
  dbHelpers.transaction("drive", orgId, (tx) => {
    tx.prepare(
      `
        INSERT INTO permissions_directory (
          id, resource_type, resource_id, resource_path, grantee_type, grantee_id,
          granted_by, begin_date_ms, expiry_date_ms, inheritable, note,
          created_at, last_modified_at, redeem_code, from_placeholder_grantee,
          metadata_type, metadata_content, external_id, external_payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
    ).run(
      newPermissionId,
      resourceType,
      resourceUUID,
      resourcePath,
      granteeType,
      granteeUUID,
      requesterId,
      data.begin_date_ms || 0,
      data.expiry_date_ms || -1,
      data.inheritable ? 1 : 0,
      data.note || "",
      currentTime,
      currentTime,
      redeem_code || null,
      null, // `from_placeholder_grantee` is null on creation
      metadataType,
      metadataContent,
      data.external_id || null,
      data.external_payload || null
    );

    const insertPermTypeStmt = tx.prepare(`
        INSERT INTO permissions_directory_types (permission_id, permission_type) VALUES (?, ?)
      `);
    for (const type of data.permission_types) {
      insertPermTypeStmt.run(newPermissionId, type);
    }
  });

  // 2. Fetch the newly created permission AFTER the transaction has successfully committed.
  const createdPermission = await getDirectoryPermissionById(
    orgId,
    newPermissionId,
    requesterId,
    true // Skip authorization check since we just created it
  );

  if (!createdPermission) {
    // If it's not found, the transaction likely failed and rolled back.
    throw new Error(
      "Failed to retrieve newly created permission after transaction."
    );
  }

  // 3. Cast the fetched permission to the front-end format and return it.
  return castToDirectoryPermissionFE(createdPermission, requesterId, orgId);
}

/**
 * Updates an existing directory permission.
 * @param orgId The drive ID.
 * @param data The request body containing updates.
 * @param requesterId The user performing the update.
 * @returns The updated DirectoryPermissionFE, or null if not found.
 */
export async function updateDirectoryPermission(
  orgId: string,
  data: {
    id: DirectoryPermission["id"];
    permission_types?: DirectoryPermissionType[];
    begin_date_ms?: number;
    expiry_date_ms?: number;
    inheritable?: boolean;
    note?: string;
    metadata?: PermissionMetadata;
    external_id?: string;
    external_payload?: string;
    redeem_code?: string;
  },
  requesterId: UserID
): Promise<DirectoryPermissionFE | null> {
  const currentTime = Date.now();

  const metadataType = data.metadata?.metadata_type || null;
  let metadataContent: string | null = null;
  if (data.metadata) {
    if ("Labels" in data.metadata.content) {
      metadataContent = data.metadata.content.Labels;
    } else if ("DirectoryPassword" in data.metadata.content) {
      metadataContent = data.metadata.content.DirectoryPassword;
    }
  }

  // 1. Run the synchronous transaction to update the database.
  dbHelpers.transaction("drive", orgId, (tx) => {
    let updateFields: string[] = [];
    let updateParams: any[] = [];

    if (data.begin_date_ms !== undefined) {
      updateFields.push("begin_date_ms = ?");
      updateParams.push(data.begin_date_ms);
    }
    if (data.expiry_date_ms !== undefined) {
      updateFields.push("expiry_date_ms = ?");
      updateParams.push(data.expiry_date_ms);
    }
    if (data.inheritable !== undefined) {
      updateFields.push("inheritable = ?");
      updateParams.push(data.inheritable ? 1 : 0);
    }
    if (data.note !== undefined) {
      updateFields.push("note = ?");
      updateParams.push(data.note);
    }
    if (data.metadata !== undefined) {
      updateFields.push("metadata_type = ?", "metadata_content = ?");
      updateParams.push(metadataType, metadataContent);
    }
    if (data.external_id !== undefined) {
      updateFields.push("external_id = ?");
      updateParams.push(data.external_id);
    }
    if (data.external_payload !== undefined) {
      updateFields.push("external_payload = ?");
      updateParams.push(data.external_payload);
    }
    if (data.redeem_code !== undefined) {
      updateFields.push("redeem_code = ?");
      updateParams.push(data.redeem_code);
    }

    // Only add last_modified_at if there are other fields to update
    if (updateFields.length > 0) {
      updateFields.push("last_modified_at = ?");
      updateParams.push(currentTime);

      const updateQuery = `
        UPDATE permissions_directory
        SET ${updateFields.join(", ")}
        WHERE id = ?
      `;
      tx.prepare(updateQuery).run(...updateParams, data.id);
    }

    if (data.permission_types !== undefined) {
      tx.prepare(
        `DELETE FROM permissions_directory_types WHERE permission_id = ?`
      ).run(data.id);
      const insertPermTypeStmt = tx.prepare(`
        INSERT INTO permissions_directory_types (permission_id, permission_type) VALUES (?, ?)
      `);
      for (const type of data.permission_types) {
        insertPermTypeStmt.run(data.id, type);
      }
    }
  });

  // 2. Fetch the updated permission after the transaction.
  const updatedPermission = await getDirectoryPermissionById(
    orgId,
    data.id,
    requesterId,
    true
  );

  if (!updatedPermission) {
    return null;
  }

  // 3. Cast to the front-end type and return.
  return castToDirectoryPermissionFE(updatedPermission, requesterId, orgId);
}

/**
 * Deletes a directory permission.
 * @param orgId The drive ID.
 * @param permissionId The ID of the permission to delete.
 * @param requesterId The user performing the deletion.
 * @returns The ID of the deleted permission, or null if not found/deleted.
 */
export async function deleteDirectoryPermission(
  orgId: string,
  permissionId: string,
  requesterId: UserID
): Promise<string | null> {
  return dbHelpers.transaction("drive", orgId, (tx) => {
    tx.prepare(
      `DELETE FROM permissions_directory_types WHERE permission_id = ?`
    ).run(permissionId);
    tx.prepare(
      `DELETE FROM permission_directory_labels WHERE permission_id = ?`
    ).run(permissionId);

    const deleteResult = tx
      .prepare(`DELETE FROM permissions_directory WHERE id = ?`)
      .run(permissionId);

    if (deleteResult.changes && deleteResult.changes > 0) {
      return permissionId;
    }
    return null;
  });
}

/**
 * Redeems a placeholder directory permission, converting it to a user-specific permission.
 * @param orgId The drive ID.
 * @param data Redemption details.
 * @returns The redeemed DirectoryPermissionFE or an error.
 */
export async function redeemDirectoryPermission(
  orgId: string,
  data: {
    permission_id: DirectoryPermission["id"];
    user_id: UserID;
    redeem_code: string;
    note?: string;
    requesterId: UserID;
  }
): Promise<{ permission?: DirectoryPermissionFE; error?: string }> {
  const currentTime = Date.now();

  const existingPermission = await getDirectoryPermissionById(
    orgId,
    data.permission_id,
    data.requesterId,
    true
  );

  if (!existingPermission) {
    return { error: "Permission not found" };
  }
  if (
    !existingPermission.granted_to.startsWith(PLACEHOLDER_GRANTEE_ID_PREFIX)
  ) {
    return { error: "Permission is not a redeemable placeholder" };
  }
  if (
    !existingPermission.redeem_code ||
    existingPermission.redeem_code !== data.redeem_code
  ) {
    return { error: "Invalid redeem code" };
  }
  if (
    existingPermission.expiry_date_ms > 0 &&
    existingPermission.expiry_date_ms <= currentTime
  ) {
    return { error: "Permission has expired" };
  }
  if (
    existingPermission.begin_date_ms > 0 &&
    existingPermission.begin_date_ms > currentTime
  ) {
    return { error: "Permission is not yet active" };
  }

  // 1. Run the synchronous transaction to update the database.
  const { type: newGranteeType, id: newGranteeId } = parseGranteeIDForDb(
    data.user_id
  );

  dbHelpers.transaction("drive", orgId, (tx) => {
    tx.prepare(
      `
      UPDATE permissions_directory
      SET
        grantee_type = ?,
        grantee_id = ?,
        redeem_code = NULL,
        from_placeholder_grantee = ?,
        note = ?,
        last_modified_at = ?
      WHERE id = ?
    `
    ).run(
      newGranteeType,
      newGranteeId,
      existingPermission.granted_to, // Store original placeholder
      data.note || existingPermission.note,
      currentTime,
      data.permission_id
    );
  });

  // 2. Fetch the updated permission after the transaction.
  const updatedPermission = await getDirectoryPermissionById(
    orgId,
    data.permission_id,
    data.requesterId,
    true
  );

  if (!updatedPermission) {
    return { error: "Failed to redeem permission: internal error." };
  }

  // 3. Cast and return the successful result.
  return {
    permission: await castToDirectoryPermissionFE(
      updatedPermission,
      data.requesterId,
      orgId
    ),
  };
}

// --- System Permission Core Logic ---

/**
 * Retrieves a system permission by its ID.
 * @param orgId The drive ID.
 * @param permissionId The ID of the permission to retrieve.
 * @param requesterId The ID of the user requesting the permission.
 * @param skipAuthorizationCheck If true, returns the raw permission without checking if `requesterId` can access _this record_.
 * @returns The raw SystemPermission object, or null if not found.
 */
export async function getSystemPermissionById(
  orgId: string,
  permissionId: string,
  requesterId: UserID,
  skipAuthorizationCheck: boolean = false
): Promise<SystemPermission | null> {
  const query = `
    SELECT
        ps.id, ps.resource_type, ps.resource_identifier, ps.grantee_type, ps.grantee_id,
        ps.granted_by, GROUP_CONCAT(pst.permission_type) AS permission_types_list,
        ps.begin_date_ms, ps.expiry_date_ms, ps.note, ps.created_at, ps.last_modified_at,
        ps.redeem_code, ps.from_placeholder_grantee, ps.metadata_type, ps.metadata_content,
        ps.external_id, ps.external_payload,
        GROUP_CONCAT(psl.label_id) AS labels_list
    FROM permissions_system ps
    LEFT JOIN permissions_system_types pst ON ps.id = pst.permission_id
    LEFT JOIN permission_system_labels psl ON ps.id = psl.permission_id
    WHERE ps.id = ?
    GROUP BY ps.id;`;

  const rows = await db.queryDrive(orgId, query, [permissionId]);

  if (rows.length === 0) {
    return null;
  }

  const rawPermission = mapDbRowToSystemPermission(rows[0]);

  if (!skipAuthorizationCheck) {
    const isOwner = (await getDriveOwnerId(orgId)) === requesterId;
    const canAccess =
      (
        await checkSystemPermissions({
          resourceTable: `TABLE_${SystemTableValueEnum.PERMISSIONS}`,
          resourceId: `${permissionId}` as SystemResourceID,
          granteeId: requesterId,
          orgId: orgId,
        })
      ).includes(SystemPermissionType.VIEW) || isOwner;
    if (!canAccess) {
      return null;
    }
  }

  return rawPermission;
}

/**
 * Lists system permissions, with pagination and authorization.
 * @param options Query options including filters, requesterId, pageSize, direction, and cursor.
 * @returns An object containing items, total count, new cursor, and authorization status.
 */
export async function listSystemPermissions(options: {
  orgId: string;
  filters?: {
    resource_ids?: string[];
    grantee_ids?: GranteeID[];
    labels?: string[]; // Assuming label values are provided for filtering
  };
  requesterId: UserID;
  pageSize: number;
  direction: SortDirection;
  cursor?: string | null;
}): Promise<{
  items: any[]; // SystemPermissionFE[]
  total: number;
  newCursor?: string | null;
  authorized: boolean;
}> {
  const { orgId, filters, requesterId, pageSize, direction, cursor } = options;
  const items: any[] = []; // SystemPermissionFE[]
  let total = 0;
  let newCursor: string | null = null;

  console.log(
    `listSystemPermissions: orgId: ${orgId}, requesterId: ${requesterId}... `
  );

  const isOwner = (await getDriveOwnerId(orgId)) === requesterId;

  let query = `
    SELECT
        ps.id, ps.resource_type, ps.resource_identifier, ps.grantee_type, ps.grantee_id,
        ps.granted_by, GROUP_CONCAT(DISTINCT pst.permission_type) AS permission_types_list,
        ps.begin_date_ms, ps.expiry_date_ms, ps.note, ps.created_at, ps.last_modified_at,
        ps.redeem_code, ps.from_placeholder_grantee, ps.metadata_type, ps.metadata_content,
        ps.external_id, ps.external_payload,
        GROUP_CONCAT(DISTINCT psl.label_id) AS labels_list
    FROM permissions_system ps
    LEFT JOIN permissions_system_types pst ON ps.id = pst.permission_id
    LEFT JOIN permission_system_labels psl ON ps.id = psl.permission_id
    WHERE 1=1
  `;

  let countQuery = `SELECT COUNT(DISTINCT id) AS count FROM permissions_system WHERE 1=1`;
  const queryParams: any[] = [];
  const countParams: any[] = [];

  if (filters?.resource_ids && filters.resource_ids.length > 0) {
    const placeholders = filters.resource_ids.map(() => "?").join(",");
    query += ` AND ps.resource_identifier IN (${placeholders})`;
    countQuery += ` AND resource_identifier IN (${placeholders})`;
    queryParams.push(...filters.resource_ids);
    countParams.push(...filters.resource_ids);
  }
  if (filters?.grantee_ids && filters.grantee_ids.length > 0) {
    const placeholders = filters.grantee_ids.map(() => "?").join(",");
    query += ` AND ps.grantee_id IN (${placeholders})`; // Note: This might need more complex parsing if grantee_id is composite
    countQuery += ` AND grantee_id IN (${placeholders})`;
    queryParams.push(...filters.grantee_ids);
    countParams.push(...filters.grantee_ids);
  }
  // Labels filtering would require joining with permission_system_labels and filtering by label_id
  // This is a simplified implementation. A robust filter for labels would be more complex.
  if (filters?.labels && filters.labels.length > 0) {
    const labelPlaceholders = filters.labels.map(() => "?").join(",");
    query += ` AND ps.id IN (SELECT permission_id FROM permission_system_labels WHERE label_id IN (${labelPlaceholders}))`;
    countQuery += ` AND id IN (SELECT permission_id FROM permission_system_labels WHERE label_id IN (${labelPlaceholders}))`;
    queryParams.push(...filters.labels);
    countParams.push(...filters.labels);
  }

  query += ` GROUP BY ps.id`; // Group by for GROUP_CONCAT

  const countRows = await db.queryDrive(orgId, countQuery, countParams);
  total = countRows[0]?.count || 0;

  let orderByClause = `ORDER BY ps.created_at ${direction}`;
  let limitClause = `LIMIT ?`;

  if (cursor) {
    const cursorTime = parseInt(cursor, 10);
    if (direction === SortDirection.ASC) {
      orderByClause = `ORDER BY ps.created_at ASC`;
      query += ` AND ps.created_at > ?`;
      queryParams.push(cursorTime);
    } else {
      orderByClause = `ORDER BY ps.created_at DESC`;
      query += ` AND ps.created_at < ?`;
      queryParams.push(cursorTime);
    }
  }

  query += ` ${orderByClause} ${limitClause}`;
  queryParams.push(pageSize + 1);

  const rows = await db.queryDrive(orgId, query, queryParams);

  for (let i = 0; i < rows.length && i < pageSize; i++) {
    const rawPerm = mapDbRowToSystemPermission(rows[i]);

    const permissions = await checkSystemPermissions({
      resourceTable: `TABLE_${SystemTableValueEnum.PERMISSIONS}`,
      granteeId: requesterId,
      orgId: orgId,
    });

    if (!isOwner && !permissions.includes(SystemPermissionType.VIEW)) {
      continue;
    }
    items.push(await castToSystemPermissionFE(rawPerm, requesterId, orgId));
  }

  if (rows.length > pageSize) {
    newCursor = rows[pageSize - 1].created_at.toString();
  }

  return { items, total, newCursor, authorized: true };
}

/**
 * Creates a new system permission.
 * @param orgId The drive ID.
 * @param data The request body containing permission details.
 * @param requesterId The user creating the permission.
 * @returns The created SystemPermissionFE.
 */
export async function createSystemPermission(
  orgId: string,
  data: IRequestCreateSystemPermission,
  requesterId: UserID
): Promise<SystemPermissionFE> {
  // Note: Returns SystemPermissionFE
  const newPermissionId = data.id || IDPrefixEnum.SystemPermission + uuidv4();
  const currentTime = Date.now();

  const { type: granteeType, id: granteeUUID } = parseGranteeIDForDb(
    data.granted_to || `${GenerateID.PlaceholderPermissionGrantee()}`
  );
  const redeem_code = data.granted_to ? null : `REDEEM_${Date.now() * 1000}`;

  let resourceType: string;
  let resourceIdentifier: string;
  if (data.resource_id.startsWith("TABLE_")) {
    resourceType = "Table";
    resourceIdentifier = data.resource_id;
  } else if (data.resource_id.startsWith("RECORD_")) {
    resourceType = "Record";
    resourceIdentifier = data.resource_id;
  } else {
    resourceType = "Unknown";
    resourceIdentifier = data.resource_id;
  }

  const metadataType = data.metadata?.metadata_type || null;
  let metadataContent: string | null = null;
  if (data.metadata) {
    if ("Labels" in data.metadata.content) {
      metadataContent = data.metadata.content.Labels;
    } else if ("DirectoryPassword" in data.metadata.content) {
      metadataContent = data.metadata.content.DirectoryPassword;
    }
  }

  // 1. Run the synchronous transaction to create the records.
  dbHelpers.transaction("drive", orgId, (tx) => {
    tx.prepare(
      `
        INSERT INTO permissions_system (
          id, resource_type, resource_identifier, grantee_type, grantee_id,
          granted_by, begin_date_ms, expiry_date_ms, note,
          created_at, last_modified_at, redeem_code, from_placeholder_grantee,
          metadata_type, metadata_content, external_id, external_payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
    ).run(
      newPermissionId,
      resourceType,
      resourceIdentifier,
      granteeType,
      granteeUUID,
      requesterId,
      data.begin_date_ms || 0,
      data.expiry_date_ms || -1,
      data.note || "",
      currentTime,
      currentTime,
      redeem_code,
      null, // from_placeholder_grantee is null on creation
      metadataType,
      metadataContent,
      data.external_id || null,
      data.external_payload || null
    );

    const insertPermTypeStmt = tx.prepare(`
        INSERT INTO permissions_system_types (permission_id, permission_type) VALUES (?, ?)
      `);
    for (const type of data.permission_types) {
      insertPermTypeStmt.run(newPermissionId, type);
    }
  });

  // 2. Fetch the newly created permission after the transaction.
  const createdPermission = await getSystemPermissionById(
    orgId,
    newPermissionId,
    requesterId,
    true
  );
  if (!createdPermission) {
    throw new Error("Failed to retrieve newly created system permission.");
  }

  const permission = await castToSystemPermissionFE(
    createdPermission,
    requesterId,
    orgId
  );

  // 3. Cast and return the result.
  return permission;
}

/**
 * Updates an existing system permission.
 * @param orgId The drive ID.
 * @param data The request body containing updates.
 * @param requesterId The user performing the update.
 * @returns The updated SystemPermissionFE, or null if not found.
 */
export async function updateSystemPermission(
  orgId: string,
  data: IRequestUpdateSystemPermission,
  requesterId: UserID
): Promise<any | null> {
  // Note: Returns SystemPermissionFE
  const currentTime = Date.now();

  const metadataType = data.metadata?.metadata_type || null;
  let metadataContent: string | null = null;
  if (data.metadata) {
    if ("Labels" in data.metadata.content) {
      metadataContent = data.metadata.content.Labels;
    } else if ("DirectoryPassword" in data.metadata.content) {
      metadataContent = data.metadata.content.DirectoryPassword;
    }
  }

  // 1. Run the synchronous transaction to update the database.
  dbHelpers.transaction("drive", orgId, (tx) => {
    let updateFields: string[] = [];
    let updateParams: any[] = [];

    if (data.resource_id !== undefined) {
      let resourceType: string;
      let resourceIdentifier: string;
      if (data.resource_id.startsWith("TABLE_")) {
        resourceType = "Table";
        resourceIdentifier = data.resource_id;
      } else if (data.resource_id.startsWith("RECORD_")) {
        resourceType = "Record";
        resourceIdentifier = data.resource_id;
      } else {
        resourceType = "Unknown";
        resourceIdentifier = data.resource_id;
      }
      updateFields.push("resource_type = ?", "resource_identifier = ?");
      updateParams.push(resourceType, resourceIdentifier);
    }
    if (data.granted_to !== undefined) {
      const { type: granteeType, id: granteeUUID } = parseGranteeIDForDb(
        data.granted_to
      );
      updateFields.push("grantee_type = ?", "grantee_id = ?");
      updateParams.push(granteeType, granteeUUID);
    }
    if (data.begin_date_ms !== undefined) {
      updateFields.push("begin_date_ms = ?");
      updateParams.push(data.begin_date_ms);
    }
    if (data.expiry_date_ms !== undefined) {
      updateFields.push("expiry_date_ms = ?");
      updateParams.push(data.expiry_date_ms);
    }
    if (data.note !== undefined) {
      updateFields.push("note = ?");
      updateParams.push(data.note);
    }
    if (data.metadata !== undefined) {
      updateFields.push("metadata_type = ?", "metadata_content = ?");
      updateParams.push(metadataType, metadataContent);
    }
    if (data.external_id !== undefined) {
      updateFields.push("external_id = ?");
      updateParams.push(data.external_id);
    }
    if (data.external_payload !== undefined) {
      updateFields.push("external_payload = ?");
      updateParams.push(data.external_payload);
    }

    if (updateFields.length > 0) {
      updateFields.push("last_modified_at = ?");
      updateParams.push(currentTime);

      const updateQuery = `
        UPDATE permissions_system
        SET ${updateFields.join(", ")}
        WHERE id = ?
      `;
      tx.prepare(updateQuery).run(...updateParams, data.id);
    }

    if (data.permission_types !== undefined) {
      tx.prepare(
        `DELETE FROM permissions_system_types WHERE permission_id = ?`
      ).run(data.id);
      const insertPermTypeStmt = tx.prepare(`
        INSERT INTO permissions_system_types (permission_id, permission_type) VALUES (?, ?)
      `);
      for (const type of data.permission_types) {
        insertPermTypeStmt.run(data.id, type);
      }
    }
  });

  // 2. Fetch the updated permission after the transaction.
  const updatedPermission = await getSystemPermissionById(
    orgId,
    data.id,
    requesterId,
    true
  );

  if (!updatedPermission) {
    return null;
  }

  // 3. Cast and return the result.
  return castToSystemPermissionFE(updatedPermission, requesterId, orgId);
}

/**
 * Deletes a system permission.
 * @param orgId The drive ID.
 * @param permissionId The ID of the permission to delete.
 * @param requesterId The user performing the deletion.
 * @returns The ID of the deleted permission, or null if not found/deleted.
 */
export async function deleteSystemPermission(
  orgId: string,
  permissionId: string,
  requesterId: UserID
): Promise<string | null> {
  return dbHelpers.transaction("drive", orgId, (tx) => {
    tx.prepare(
      `DELETE FROM permissions_system_types WHERE permission_id = ?`
    ).run(permissionId);
    tx.prepare(
      `DELETE FROM permission_system_labels WHERE permission_id = ?`
    ).run(permissionId);

    const deleteResult = tx
      .prepare(`DELETE FROM permissions_system WHERE id = ?`)
      .run(permissionId);

    if (deleteResult.changes && deleteResult.changes > 0) {
      return permissionId;
    }
    return null;
  });
}

/**
 * Redeems a placeholder system permission, converting it to a user-specific permission.
 * @param orgId The drive ID.
 * @param data Redemption details.
 * @returns The redeemed SystemPermissionFE or an error.
 */
export async function redeemSystemPermission(
  orgId: string,
  data: IRequestRedeemSystemPermission,
  requesterId: UserID
): Promise<{ permission?: any; error?: string }> {
  // Note: Returns SystemPermissionFE
  const currentTime = Date.now();

  const existingPermission = await getSystemPermissionById(
    orgId,
    data.permission_id,
    requesterId,
    true
  );

  if (!existingPermission) {
    return { error: "Permission not found" };
  }
  if (
    !existingPermission.granted_to.startsWith(PLACEHOLDER_GRANTEE_ID_PREFIX)
  ) {
    return { error: "Permission is not a redeemable placeholder" };
  }
  if (
    !existingPermission.redeem_code ||
    existingPermission.redeem_code !== data.redeem_code
  ) {
    return { error: "Invalid redeem code" };
  }
  if (
    existingPermission.expiry_date_ms > 0 &&
    existingPermission.expiry_date_ms <= currentTime
  ) {
    return { error: "Permission has expired" };
  }

  // 1. Run the synchronous transaction to update the database.
  const { type: newGranteeType, id: newGranteeId } = parseGranteeIDForDb(
    data.user_id
  );

  dbHelpers.transaction("drive", orgId, (tx) => {
    tx.prepare(
      `
      UPDATE permissions_system
      SET
        grantee_type = ?,
        grantee_id = ?,
        redeem_code = NULL,
        from_placeholder_grantee = ?,
        note = ?,
        last_modified_at = ?
      WHERE id = ?
    `
    ).run(
      newGranteeType,
      newGranteeId,
      existingPermission.granted_to, // Store original placeholder
      data.note || existingPermission.note,
      currentTime,
      data.permission_id
    );
  });

  // 2. Fetch the updated permission after the transaction.
  const updatedPermission = await getSystemPermissionById(
    orgId,
    data.permission_id,
    requesterId,
    true
  );

  if (!updatedPermission) {
    return { error: "Failed to redeem permission: internal error." };
  }

  // 3. Cast and return the successful result.
  return {
    permission: await castToSystemPermissionFE(
      updatedPermission,
      requesterId,
      orgId
    ),
  };
}

// Directory Permissions Handlers

export async function getDirectoryPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string; directory_permission_id: string };
  }>,
  reply: FastifyReply
) {
  const { org_id, directory_permission_id } = request.params;
  const requesterApiKey = await authenticateRequest(request, "drive", org_id);
  if (!requesterApiKey) {
    return reply
      .status(401)
      .send(
        createApiResponse(undefined, { code: 401, message: "Unauthorized" })
      );
  }

  const requesterId = requesterApiKey.user_id;

  try {
    const permission = await getDirectoryPermissionById(
      org_id,
      directory_permission_id,
      requesterId
    );
    if (permission) {
      const permissionFE = await castToDirectoryPermissionFE(
        permission,
        requesterId,
        org_id
      );
      reply.status(200).send({
        ok: { data: permissionFE } as IResponseGetDirectoryPermission["ok"],
      });
    } else {
      reply.status(404).send({
        err: { code: 404, message: "Directory permission not found." },
      });
    }
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function listDirectoryPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestListDirectoryPermissions;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const { filters, page_size, direction, cursor } = request.body;

  const requesterApiKey = await authenticateRequest(request, "drive", org_id);
  if (!requesterApiKey) {
    return reply
      .status(401)
      .send(
        createApiResponse(undefined, { code: 401, message: "Unauthorized" })
      );
  }

  const requesterId = requesterApiKey.user_id;

  try {
    if (!filters?.resource_id) {
      reply.status(400).send({
        err: {
          code: 400,
          message:
            "resource_id is required in filters for listing directory permissions.",
        },
      });
      return;
    }

    const result = await listDirectoryPermissionsForResource({
      orgId: org_id,
      resourceId: filters.resource_id,
      requesterId,
      pageSize: page_size || 10,
      direction: direction || SortDirection.ASC,
      cursor,
    });

    if (!result.authorized) {
      reply.status(403).send({
        err: {
          code: 403,
          message: "Unauthorized to list permissions for this resource.",
        },
      });
      return;
    }

    reply.status(200).send({
      ok: {
        data: {
          items: result.items,
          page_size: page_size || 10,
          total: result.total,
          cursor: result.newCursor,
        },
      } as IResponseListDirectoryPermissions["ok"],
    });
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function createDirectoryPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestCreateDirectoryPermission;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const data = request.body;

  const requesterApiKey = await authenticateRequest(request, "drive", org_id);
  if (!requesterApiKey) {
    return reply
      .status(401)
      .send(
        createApiResponse(undefined, { code: 401, message: "Unauthorized" })
      );
  }

  const requesterId = requesterApiKey.user_id;

  try {
    const createdPermissionFE = await createDirectoryPermission(
      org_id,
      data,
      requesterId
    );
    reply.status(200).send({
      ok: {
        data: { permission: createdPermissionFE },
      } as IResponseCreateDirectoryPermission["ok"],
    });
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function updateDirectoryPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestUpdateDirectoryPermission;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const data = request.body;

  const requesterApiKey = await authenticateRequest(request, "drive", org_id);
  if (!requesterApiKey) {
    return reply
      .status(401)
      .send(
        createApiResponse(undefined, { code: 401, message: "Unauthorized" })
      );
  }

  const requesterId = requesterApiKey.user_id;

  try {
    const updatedPermissionFE = await updateDirectoryPermission(
      org_id,
      data,
      requesterId
    );
    if (updatedPermissionFE) {
      reply.status(200).send({
        ok: {
          data: { permission: updatedPermissionFE },
        } as IResponseUpdateDirectoryPermission["ok"],
      });
    } else {
      reply.status(404).send({
        err: {
          code: 404,
          message: "Directory permission not found or failed to update.",
        },
      });
    }
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function deleteDirectoryPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestDeleteDirectoryPermission;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const { permission_id } = request.body;

  const requesterApiKey = await authenticateRequest(request, "drive", org_id);
  if (!requesterApiKey) {
    return reply
      .status(401)
      .send(
        createApiResponse(undefined, { code: 401, message: "Unauthorized" })
      );
  }

  const requesterId = requesterApiKey.user_id;

  try {
    const deletedId = await deleteDirectoryPermission(
      org_id,
      permission_id,
      requesterId
    );
    if (deletedId) {
      reply.status(200).send({
        ok: {
          data: { deleted_id: deletedId },
        } as IResponseDeleteDirectoryPermission["ok"],
      });
    } else {
      reply.status(404).send({
        err: {
          code: 404,
          message: "Directory permission not found or failed to delete.",
        },
      });
    }
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function checkDirectoryPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestCheckDirectoryPermissions;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const { resource_id, grantee_id } = request.body;

  const requesterApiKey = await authenticateRequest(request, "drive", org_id);
  if (!requesterApiKey) {
    return reply
      .status(401)
      .send(
        createApiResponse(undefined, { code: 401, message: "Unauthorized" })
      );
  }

  const requesterId = requesterApiKey.user_id;

  try {
    if (!resource_id || !grantee_id) {
      reply.status(400).send({
        err: {
          code: 400,
          message: "resource_id and grantee_id are required.",
        },
      });
      return;
    }

    // The core logic for checking directory permissions including inheritance
    // is already implemented in the `checkDirectoryPermissions` service function.
    const permissions = await checkDirectoryPermissions(
      resource_id,
      grantee_id,
      org_id
    );

    reply.status(200).send({
      ok: {
        data: { resource_id, grantee_id, permissions },
      } as IResponseCheckDirectoryPermissions["ok"],
    });
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function redeemDirectoryPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestRedeemDirectoryPermission;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const data = request.body;

  const requesterApiKey = await authenticateRequest(request, "drive", org_id);
  if (!requesterApiKey) {
    return reply
      .status(401)
      .send(
        createApiResponse(undefined, { code: 401, message: "Unauthorized" })
      );
  }

  const requesterId = requesterApiKey.user_id;

  try {
    const result = await redeemDirectoryPermission(org_id, {
      ...data,
      requesterId,
    });
    if (result.permission) {
      reply.status(200).send({
        ok: {
          data: { permission: result.permission },
        } as IResponseRedeemDirectoryPermission["ok"],
      });
    } else {
      reply.status(400).send({
        err: {
          code: 400,
          message: result.error || "Failed to redeem directory permission.",
        },
      });
    }
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

// System Permissions Handlers

export async function getSystemPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string; system_permission_id: string };
  }>,
  reply: FastifyReply
) {
  const { org_id, system_permission_id } = request.params;

  const requesterApiKey = await authenticateRequest(request, "drive", org_id);
  if (!requesterApiKey) {
    return reply
      .status(401)
      .send(
        createApiResponse(undefined, { code: 401, message: "Unauthorized" })
      );
  }

  const requesterId = requesterApiKey.user_id;

  try {
    const permission = await getSystemPermissionById(
      org_id,
      system_permission_id,
      requesterId
    );
    if (permission) {
      const permissionFE = await castToSystemPermissionFE(
        permission,
        requesterId,
        org_id
      );
      reply.status(200).send({
        ok: { data: permissionFE } as IResponseGetSystemPermission["ok"],
      });
    } else {
      reply
        .status(404)
        .send({ err: { code: 404, message: "System permission not found." } });
    }
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function listSystemPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestListSystemPermissions;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const { filters, page_size, direction, cursor } = request.body;

  const requesterApiKey = await authenticateRequest(request, "drive", org_id);
  if (!requesterApiKey) {
    return reply
      .status(401)
      .send(
        createApiResponse(undefined, { code: 401, message: "Unauthorized" })
      );
  }

  const requesterId = requesterApiKey.user_id;

  try {
    const result = await listSystemPermissions({
      orgId: org_id,
      filters,
      requesterId,
      pageSize: page_size || 10,
      direction: direction || SortDirection.ASC,
      cursor,
    });

    if (!result.authorized) {
      reply.status(403).send({
        err: {
          code: 403,
          message: "Unauthorized to list system permissions.",
        },
      });
      return;
    }

    reply.status(200).send({
      ok: {
        data: {
          items: result.items,
          page_size: page_size || 10,
          total: result.total,
          cursor: result.newCursor,
        },
      } as IResponseListSystemPermissions["ok"],
    });
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function createSystemPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestCreateSystemPermission;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const data = request.body;

  const requesterApiKey = await authenticateRequest(request, "drive", org_id);
  if (!requesterApiKey) {
    return reply
      .status(401)
      .send(
        createApiResponse(undefined, { code: 401, message: "Unauthorized" })
      );
  }

  const requesterId = requesterApiKey.user_id;

  try {
    const createdPermissionFE = await createSystemPermission(
      org_id,
      data,
      requesterId
    );
    reply.status(200).send({
      ok: {
        data: { permission: createdPermissionFE },
      } as IResponseCreateSystemPermission["ok"],
    });
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function updateSystemPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestUpdateSystemPermission;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const data = request.body;

  const requesterApiKey = await authenticateRequest(request, "drive", org_id);
  if (!requesterApiKey) {
    return reply
      .status(401)
      .send(
        createApiResponse(undefined, { code: 401, message: "Unauthorized" })
      );
  }

  const requesterId = requesterApiKey.user_id;

  try {
    const updatedPermissionFE = await updateSystemPermission(
      org_id,
      data,
      requesterId
    );
    if (updatedPermissionFE) {
      reply.status(200).send({
        ok: {
          data: updatedPermissionFE,
        } as IResponseUpdateSystemPermission["ok"],
      });
    } else {
      reply.status(404).send({
        err: {
          code: 404,
          message: "System permission not found or failed to update.",
        },
      });
    }
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function deleteSystemPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestDeleteSystemPermission;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const { permission_id } = request.body;

  const requesterApiKey = await authenticateRequest(request, "drive", org_id);
  if (!requesterApiKey) {
    return reply
      .status(401)
      .send(
        createApiResponse(undefined, { code: 401, message: "Unauthorized" })
      );
  }

  const requesterId = requesterApiKey.user_id;

  try {
    const deletedId = await deleteSystemPermission(
      org_id,
      permission_id,
      requesterId
    );
    if (deletedId) {
      reply.status(200).send({
        ok: {
          data: { deleted_id: deletedId },
        } as IResponseDeleteSystemPermission["ok"],
      });
    } else {
      reply.status(404).send({
        err: {
          code: 404,
          message: "System permission not found or failed to delete.",
        },
      });
    }
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function checkSystemPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestCheckSystemPermissions;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const { resource_id, grantee_id } = request.body;

  const requesterApiKey = await authenticateRequest(request, "drive", org_id);
  if (!requesterApiKey) {
    return reply
      .status(401)
      .send(
        createApiResponse(undefined, { code: 401, message: "Unauthorized" })
      );
  }

  const requesterId = requesterApiKey.user_id;

  try {
    if (!resource_id || !grantee_id) {
      reply.status(400).send({
        err: {
          code: 400,
          message: "resource_id and grantee_id are required.",
        },
      });
      return;
    }

    const permissions: SystemPermissionType[] = [];

    if (resource_id.startsWith("TABLE_")) {
      const _permissions = await checkSystemPermissions({
        resourceTable: resource_id as `TABLE_${SystemTableValueEnum}`,
        granteeId: grantee_id,
        orgId: org_id,
      });
      permissions.push(..._permissions);
    } else {
      const tableSlug = castIdToTable(resource_id);
      const _permissions = await checkSystemPermissions({
        resourceTable: `TABLE_${tableSlug}`,
        resourceId: `${resource_id}` as SystemResourceID,
        granteeId: grantee_id,
        orgId: org_id,
      });
      permissions.push(..._permissions);
    }

    reply.status(200).send({
      ok: {
        data: { resource_id, grantee_id, permissions },
      } as IResponseCheckSystemPermissions["ok"],
    });
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}

export async function redeemSystemPermissionsHandler(
  request: FastifyRequest<{
    Params: { org_id: string };
    Body: IRequestRedeemSystemPermission;
  }>,
  reply: FastifyReply
) {
  const { org_id } = request.params;
  const data = request.body;

  const requesterApiKey = await authenticateRequest(request, "drive", org_id);
  if (!requesterApiKey) {
    return reply
      .status(401)
      .send(
        createApiResponse(undefined, { code: 401, message: "Unauthorized" })
      );
  }

  const requesterId = requesterApiKey.user_id;

  try {
    const result = await redeemSystemPermission(org_id, data, requesterId);
    if (result.permission) {
      reply.status(200).send({
        ok: {
          data: result.permission,
        } as IResponseRedeemSystemPermission["ok"],
      });
    } else {
      reply.status(400).send({
        err: {
          code: 400,
          message: result.error || "Failed to redeem system permission.",
        },
      });
    }
  } catch (error: any) {
    reply.status(500).send({
      err: { code: 500, message: error.message || "Internal server error." },
    });
  }
}
