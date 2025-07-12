// src/routes/v1/drive/labels/handlers.ts
import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import {
  Label,
  LabelFE,
  LabelID,
  LabelValue, // This is LabelStringValue in Rust
  UserID,
  DriveID, // For OrgIdParams
  IDPrefixEnum,
  IRequestGetLabel,
  IRequestListLabels,
  IRequestCreateLabel,
  IRequestUpdateLabel,
  IRequestDeleteLabel,
  IRequestLabelResource,
  IResponseGetLabel,
  IResponseListLabels,
  IResponseCreateLabel,
  IResponseUpdateLabel,
  IResponseDeleteLabel,
  IResponseLabelResource,
  ISuccessResponse,
  IErrorResponse,
  IPaginatedResponse,
  SystemPermissionType, // For permission_previews
  SortDirection,
  WebhookEventLabel,
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import { authenticateRequest } from "../../../../services/auth"; // Removed generateApiKey as it's not used in this file
import { getDriveOwnerId, OrgIdParams } from "../../types";

interface GetLabelParams extends OrgIdParams {
  label_id: string; // Can be LabelID or LabelValue
}

// Re-using IRequestListLabels, IRequestCreateLabel, IRequestUpdateLabel, IRequestDeleteLabel, IRequestLabelResource from @officexapp/types

// --- Helper Functions (Re-used or newly defined) ---

function createApiResponse<T>(
  data?: T,
  error?: { code: number; message: string }
): ISuccessResponse<T> | IErrorResponse {
  if (error) {
    return {
      err: {
        code: error.code,
        message: error.message,
      },
    };
  } else {
    // Cast to ISuccessResponse<T> since data is guaranteed to be present if no error
    return {
      ok: {
        data: data as T,
      },
    };
  }
}

// TODO: PERMIT Implement actual permission checking logic based on permissions_system and permissions_directory tables
// This is a complex piece of logic from Rust's `check_system_permissions` and `check_system_resource_permissions_labels`
// For now, providing a simplified mock.
async function checkSystemPermissions(
  orgId: DriveID,
  resourceIdentifier: string, // Corresponds to SystemResourceID in Rust (e.g., "LABELS" or "LabelID_xyz")
  granteeId: UserID, // Corresponds to PermissionGranteeID::User in Rust
  labelValue?: string // Used in check_system_resource_permissions_labels in Rust for label-prefixed permissions
): Promise<SystemPermissionType[]> {
  // Mock permissions: Owner always has all. Others might have some.
  const ownerId = await getDriveOwnerId(orgId);
  if (granteeId === ownerId) {
    return [
      SystemPermissionType.CREATE,
      SystemPermissionType.EDIT,
      SystemPermissionType.DELETE,
      SystemPermissionType.VIEW,
      SystemPermissionType.INVITE,
    ];
  }

  // TODO: PERMIT Implement actual logic to query 'permissions_system' table.
  // This mock grants VIEW for 'LABELS' table and any specific Label record by default for non-owners.
  // It also grants EDIT for 'LABELS' table.
  let permissions: SystemPermissionType[] = [];

  // Check general table-level permissions
  if (resourceIdentifier === "LABELS") {
    permissions.push(SystemPermissionType.VIEW);
    // Example: Grant EDIT permission at table level for certain users
    // if (granteeId === "UserID_some_admin") {
    // permissions.push(SystemPermissionType.EDIT);
    // }
  }
  // Check record-level permissions for labels
  else if (resourceIdentifier.startsWith(IDPrefixEnum.LabelID)) {
    permissions.push(SystemPermissionType.VIEW);
    // Example: Grant EDIT permission for specific label if explicitly allowed
    // if (granteeId === "UserID_specific_user_for_label_edit" && resourceIdentifier === "LabelID_xyz") {
    // permissions.push(SystemPermissionType.EDIT);
    // }
  }
  // Check permissions for other resources (files, folders, etc.) when a label is being applied/removed
  // This is a simplified check for `SystemPermissionType.EDIT` on the resource itself.
  else if (resourceIdentifier.includes("_")) {
    // Assuming it's a resource ID like "FileID_..."
    // This part is a simplified placeholder and would need granular checks in a real system.
    permissions.push(SystemPermissionType.EDIT); // Allow edit for any resource when labeling, for testing
  }

  return permissions;
}

// --- Validation Functions (adapted from Rust) ---

function validateIdString(
  id: string,
  fieldName: string
): { valid: boolean; error?: string } {
  if (!id || id.length === 0 || id.length > 256) {
    // Max size from LabelID Storable bound in Rust
    return {
      valid: false,
      error: `${fieldName} is required and must be less than 256 characters.`,
    };
  }
  return { valid: true };
}

function validateShortString(
  str: string,
  fieldName: string
): { valid: boolean; error?: string } {
  if (!str || str.length === 0 || str.length > 256) {
    // Max size from LabelStringValue Storable bound
    return {
      valid: false,
      error: `${fieldName} is required and must be less than 256 characters.`,
    };
  }
  return { valid: true };
}

function validateDescription(
  str: string,
  fieldName: string
): { valid: boolean; error?: string } {
  if (str.length > 1024) {
    // Assuming a larger max size for description/note
    return {
      valid: false,
      error: `${fieldName} must be less than 1024 characters.`,
    };
  }
  return { valid: true };
}

function validateExternalId(id: string): { valid: boolean; error?: string } {
  if (id.length > 256) {
    return {
      valid: false,
      error: "external_id must be less than 256 characters.",
    };
  }
  return { valid: true };
}

function validateExternalPayload(payload: string): {
  valid: boolean;
  error?: string;
} {
  // In Rust it's a String, so assuming it can be long, but validate a reasonable max.
  if (payload.length > 4096) {
    // Arbitrary large limit
    return {
      valid: false,
      error: "external_payload must be less than 4096 characters.",
    };
  }
  return { valid: true };
}

function validateLabelValue(labelValue: string): {
  valid: boolean;
  error?: string;
  validatedValue?: LabelValue;
} {
  if (labelValue.length === 0) {
    return { valid: false, error: "Label cannot be empty" };
  }
  if (labelValue.length > 64) {
    return { valid: false, error: "Label cannot exceed 64 characters" };
  }
  // Check characters: alphanumeric and underscores only
  if (!/^[a-zA-Z0-9_]+$/.test(labelValue)) {
    return {
      valid: false,
      error: "Label can only contain alphanumeric characters and underscores",
    };
  }
  return {
    valid: true,
    validatedValue: labelValue.toLowerCase() as LabelValue,
  }; // Convert to lowercase
}

function validateColor(color: string): {
  valid: boolean;
  error?: string;
  validatedColor?: string;
} {
  if (color.length === 0) {
    return { valid: false, error: "Color cannot be empty" };
  }
  // Allow 4-char (#RGB) or 7-char (#RRGGBB) hex codes
  if (!color.startsWith("#") || !(color.length === 4 || color.length === 7)) {
    return {
      valid: false,
      error:
        "Color must start with '#' and be 4 or 7 characters long (e.g., #RRGGBB or #RGB)",
    };
  }
  if (!/^[0-9A-Fa-f]+$/.test(color.substring(1))) {
    return { valid: false, error: "Color must be a valid hex code" };
  }
  return { valid: true, validatedColor: color.toUpperCase() }; // Convert to uppercase for consistency
}

// From Rust `parse_label_resource_id`
function parseLabelResourceID(idStr: string): {
  valid: boolean;
  resourceId?: string;
  resourceTypePrefix?: string;
  error?: string;
} {
  const parts = idStr.split("_");
  if (parts.length < 2) {
    return { valid: false, error: `Malformed resource ID: ${idStr}` };
  }

  // Reconstruct prefix string to match IDPrefixEnum values
  const prefixCandidate = parts[0] + "_"; // e.g., "File" + "_" -> "FileID_"

  // Map candidate prefixes to IDPrefixEnum values
  let foundPrefix: IDPrefixEnum | undefined;
  for (const enumKey in IDPrefixEnum) {
    const enumValue = IDPrefixEnum[enumKey as keyof typeof IDPrefixEnum];
    // Check if the enum value is a prefix for the ID string
    if (idStr.startsWith(enumValue)) {
      foundPrefix = enumValue;
      break;
    }
  }

  if (foundPrefix) {
    // The actual resource ID is the full string (e.g., "FileID_uuid")
    // The resourceTypePrefix is the enum value without the trailing '_' for easier mapping to table names
    const typeWithoutTrailingUnderscore = foundPrefix.slice(0, -1);
    return {
      valid: true,
      resourceId: idStr,
      resourceTypePrefix: typeWithoutTrailingUnderscore,
    };
  } else {
    return { valid: false, error: `Invalid resource ID prefix: ${parts[0]}` };
  }
}

// TODO: DRIVE Implement placeholder for `update_external_id_mapping`
async function updateExternalIdMapping(
  oldExternalId: string | null | undefined,
  newExternalId: string | null | undefined,
  internalId: string | null | undefined,
  orgId: DriveID // Need orgId to access the drive's external_id_mapping table
): Promise<void> {
  // In Rust, this modifies a stable BTreeMap.
  // In SQLite, you'd likely have a separate table for external ID mappings (e.g., `external_id_mappings`)
  // or store external_id directly on the main record and ensure uniqueness via unique index.
  // For now, this is a no-op placeholder.
  console.log(
    `TODO: DRIVE Implement updateExternalIdMapping for org ${orgId}: old=${oldExternalId}, new=${newExternalId}, internal=${internalId}`
  );
}

// TODO: WEBHOOK Implement placeholder for webhook firing logic
async function fireLabelWebhook(
  event: WebhookEventLabel,
  webhooks: any[], // TODO: Define Webhook type from Rust
  oldData: any | null, // Pre-state data
  newData: any | null, // Post-state data
  notes: string | null,
  orgId: DriveID
): Promise<void> {
  console.log(
    `TODO: WEBHOOK Implement fireLabelWebhook for org ${orgId}: event=${event}, notes=${notes}`
  );
  // This would involve fetching active webhooks from the 'webhooks' table,
  // filtering them by event and potentially by label,
  // then making HTTP requests to the webhook URLs.
}

// TODO: WEBHOOK Implement placeholder for getting active webhooks (Rust: `get_active_label_webhooks`)
async function getActiveLabelWebhooks(
  labelId: LabelID,
  event: WebhookEventLabel,
  orgId: DriveID
): Promise<any[]> {
  // Returns list of Webhook objects
  console.log(
    `TODO: WEBHOOK Implement getActiveLabelWebhooks for org ${orgId}: labelId=${labelId}, event=${event}`
  );
  // This would query the 'webhooks' table, filter by event, and potentially by associated labels.
  return []; // Mock empty array
}

// --- Handlers ---

export async function getLabelHandler(
  request: FastifyRequest<{ Params: GetLabelParams }>,
  reply: FastifyReply
): Promise<void> {
  const { org_id, label_id: requestedLabelIdentifier } = request.params;

  try {
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    let label: Label | undefined;
    let queryResult: any[] = [];

    // Determine if requestedLabelIdentifier is an ID or a Value
    if (requestedLabelIdentifier.startsWith(IDPrefixEnum.LabelID)) {
      // It's a LabelID
      queryResult = await db.queryDrive(
        org_id,
        "SELECT id, value, public_note, private_note, color, created_by_user_id, created_at, last_updated_at, external_id, external_payload FROM labels WHERE id = ?",
        [requestedLabelIdentifier]
      );
    } else {
      // It's a LabelStringValue
      // First, get the LabelID from the value
      const labelIdResult = await db.queryDrive(
        org_id,
        "SELECT id FROM labels WHERE value = ?",
        [requestedLabelIdentifier.toLowerCase()] // Rust converts to lowercase for consistency
      );

      if (labelIdResult.length > 0) {
        const actualLabelId = labelIdResult[0].id;
        queryResult = await db.queryDrive(
          org_id,
          "SELECT id, value, public_note, private_note, color, created_by_user_id, created_at, last_updated_at, external_id, external_payload FROM labels WHERE id = ?",
          [actualLabelId]
        );
      }
    }

    if (!queryResult || queryResult.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Label not found",
        })
      );
    }

    const rawLabel = queryResult[0];

    // Fetch associated resources from junction tables
    const junctionTables = [
      "api_key_labels",
      "contact_labels",
      "file_labels",
      "folder_labels",
      "disk_labels",
      "drive_labels",
      "group_labels",
      "group_invite_labels",
      "permission_directory_labels",
      "permission_system_labels",
      "webhook_labels",
      // label_labels is for nested labels, not resources *this* label is applied to
    ];

    const associatedResources: string[] = []; // Collect resource IDs
    for (const tableName of junctionTables) {
      try {
        let resourceIdColumn: string;
        // Determine the resource ID column name based on table (this is simplified, needs better mapping)
        if (tableName.includes("api_key")) resourceIdColumn = "api_key_id";
        else if (tableName.includes("contact")) resourceIdColumn = "user_id";
        else if (tableName.includes("file")) resourceIdColumn = "file_id";
        else if (tableName.includes("folder")) resourceIdColumn = "folder_id";
        else if (tableName.includes("disk")) resourceIdColumn = "disk_id";
        else if (tableName.includes("drive")) resourceIdColumn = "drive_id";
        else if (tableName.includes("group_invite"))
          resourceIdColumn = "invite_id";
        else if (tableName.includes("group")) resourceIdColumn = "group_id";
        else if (tableName.includes("directory"))
          resourceIdColumn = "permission_id";
        else if (tableName.includes("system"))
          resourceIdColumn = "permission_id";
        else if (tableName.includes("webhook")) resourceIdColumn = "webhook_id";
        else continue; // Skip if unable to determine column

        const res = await db.queryDrive(
          org_id,
          `SELECT ${resourceIdColumn} FROM ${tableName} WHERE label_id = ?`,
          [rawLabel.id]
        );
        res.forEach((row: any) => {
          // Add appropriate prefix to resource ID
          if (row[resourceIdColumn]) {
            let prefix = "";
            if (resourceIdColumn === "api_key_id") prefix = IDPrefixEnum.ApiKey;
            else if (resourceIdColumn === "user_id") prefix = IDPrefixEnum.User;
            else if (resourceIdColumn === "file_id") prefix = IDPrefixEnum.File;
            else if (resourceIdColumn === "folder_id")
              prefix = IDPrefixEnum.Folder;
            else if (resourceIdColumn === "disk_id") prefix = IDPrefixEnum.Disk;
            else if (resourceIdColumn === "drive_id")
              prefix = IDPrefixEnum.Drive;
            else if (resourceIdColumn === "invite_id")
              prefix = IDPrefixEnum.GroupInvite;
            else if (resourceIdColumn === "group_id")
              prefix = IDPrefixEnum.Group;
            else if (
              resourceIdColumn === "permission_id" &&
              tableName.includes("directory")
            )
              prefix = IDPrefixEnum.DirectoryPermission;
            else if (
              resourceIdColumn === "permission_id" &&
              tableName.includes("system")
            )
              prefix = IDPrefixEnum.SystemPermission;
            else if (resourceIdColumn === "webhook_id")
              prefix = IDPrefixEnum.Webhook;
            else if (resourceIdColumn === "parent_label_id")
              prefix = IDPrefixEnum.LabelID; // Special case for label_labels where this label is a parent

            // Only add if not already prefixed, or if the prefix matches current schema for IDs
            if (row[resourceIdColumn].startsWith(prefix)) {
              associatedResources.push(row[resourceIdColumn]);
            } else {
              // This handles cases where the raw ID from DB might not have the full prefix
              // e.g., if DB stores "uuid" but type is "TypeID_uuid"
              associatedResources.push(
                `${prefix}${row[resourceIdColumn].split("_")[1] || row[resourceIdColumn]}`
              );
            }
          }
        });
      } catch (e) {
        request.log.warn(`Could not query junction table ${tableName}: ${e}`);
      }
    }

    // Fetch nested labels (labels applied to this label) from label_labels junction table
    const nestedLabels: string[] = []; // Stores LabelValue (string)
    try {
      const nestedLabelResults = await db.queryDrive(
        org_id,
        `SELECT T2.value FROM label_labels AS T1 JOIN labels AS T2 ON T1.child_label_id = T2.id WHERE T1.parent_label_id = ?`,
        [rawLabel.id]
      );
      nestedLabelResults.forEach((row: any) => nestedLabels.push(row.value));
    } catch (e) {
      request.log.warn(`Could not query label_labels for nested labels: ${e}`);
    }

    label = {
      id: rawLabel.id as LabelID,
      value: rawLabel.value as LabelValue,
      public_note: rawLabel.public_note,
      private_note: rawLabel.private_note,
      color: rawLabel.color,
      created_by: rawLabel.created_by_user_id as UserID,
      created_at: rawLabel.created_at,
      last_updated_at: rawLabel.last_updated_at,
      resources: associatedResources, // Populated
      labels: nestedLabels, // Populated
      external_id: rawLabel.external_id,
      external_payload: rawLabel.external_payload,
    } as Label;

    // Check permissions if not owner
    if (!isOwner) {
      const tablePermissions = await checkSystemPermissions(
        org_id,
        `LABELS`, // SystemTableEnum::Labels in Rust
        requesterApiKey.user_id,
        label.value // For label-prefixed permissions
      );

      const resourcePermissions = await checkSystemPermissions(
        org_id,
        `${IDPrefixEnum.LabelID}${label.id.split("_")[1]}`, // SystemRecordIDEnum::Label in Rust
        requesterApiKey.user_id,
        label.value
      );

      if (
        !tablePermissions.includes(SystemPermissionType.VIEW) &&
        !resourcePermissions.includes(SystemPermissionType.VIEW)
      ) {
        return reply
          .status(403)
          .send(
            createApiResponse(undefined, { code: 403, message: "Forbidden" })
          );
      }
    }

    // Cast to FE type and apply redaction
    const labelFE = await castLabelToLabelFE(
      label,
      requesterApiKey.user_id,
      org_id
    );

    return reply.status(200).send(createApiResponse<LabelFE>(labelFE));
  } catch (error) {
    request.log.error("Error in getLabelHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function listLabelsHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestListLabels }>,
  reply: FastifyReply
): Promise<void> {
  const { org_id } = request.params;

  try {
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    const requestBody = request.body;

    // Validate request body
    const validationErrors: string[] = [];
    if (requestBody.page_size !== undefined) {
      if (requestBody.page_size === 0 || requestBody.page_size > 1000) {
        validationErrors.push("page_size must be between 1 and 1000");
      }
    }
    if (requestBody.filters?.prefix !== undefined) {
      const validation = validateShortString(
        requestBody.filters.prefix,
        "filters.prefix"
      );
      if (!validation.valid) {
        validationErrors.push(validation.error!);
      }
    }
    if (requestBody.cursor !== undefined) {
      const validation = validateIdString(requestBody.cursor, "cursor");
      if (!validation.valid) {
        validationErrors.push(validation.error!);
      }
    }

    if (validationErrors.length > 0) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validationErrors.join("; "),
        })
      );
    }

    const prefixFilter = requestBody.filters?.prefix?.toLowerCase() || "";

    // Check table-level permission for the prefix
    const tablePermissions = await checkSystemPermissions(
      org_id,
      `LABELS`, // SystemTableEnum::Labels in Rust
      requesterApiKey.user_id,
      prefixFilter // For label-prefixed permissions
    );
    const hasTablePermission = tablePermissions.includes(
      SystemPermissionType.VIEW
    );

    request.log.debug(`has_table_permission: ${hasTablePermission}`);

    if (!hasTablePermission && !isOwner) {
      // If not owner and no table-level permission for view
      // Return empty list if no general view permission
      return reply.status(200).send(
        createApiResponse<IPaginatedResponse<LabelFE>>({
          items: [],
          page_size: requestBody.page_size || 50,
          total: 0,
          cursor: null,
        })
      );
    }

    let query =
      "SELECT id, value, public_note, private_note, color, created_by_user_id, created_at, last_updated_at, external_id, external_payload FROM labels";
    const params: any[] = [];
    const conditions: string[] = [];

    if (prefixFilter) {
      conditions.push("value LIKE ?");
      params.push(`${prefixFilter}%`);
    }

    // Apply cursor-based pagination
    const orderByField = "created_at"; // Rust's LABELS_BY_TIME_LIST implies sorting by creation time.
    const direction = requestBody.direction || SortDirection.DESC; // Default to DESC as per Rust example

    let cursorCondition = "";
    let cursorValue: number | null = null;
    if (requestBody.cursor) {
      cursorValue = Number(requestBody.cursor);
      if (!isNaN(cursorValue)) {
        cursorCondition = ` ${orderByField} ${direction === SortDirection.DESC ? "<" : ">"} ?`; // Adjust operator based on direction
        params.push(cursorValue);
      }
    }

    if (conditions.length > 0 || cursorCondition) {
      query += " WHERE " + conditions.join(" AND ");
      if (cursorCondition) {
        query += (conditions.length > 0 ? " AND" : "") + cursorCondition;
      }
    }

    query += ` ORDER BY ${orderByField} ${direction}`;

    const pageSize = requestBody.page_size || 50;
    query += ` LIMIT ${pageSize + 1}`; // Fetch one more to check for next cursor

    request.log.debug(`List labels query: ${query}, params: ${params}`);
    const rawLabels = await db.queryDrive(org_id, query, params);

    let labels: Label[] = [];
    let nextCursor: string | null = null;
    let previousCursor: string | null = null; // For cursor_up logic

    if (rawLabels.length > pageSize) {
      const extraItem = rawLabels.pop(); // Remove the extra item
      if (extraItem) {
        nextCursor = String(extraItem.created_at); // Cursor for next page
      }
    }

    // Process labels, applying resource-level permissions and redaction
    for (const rawLabel of rawLabels) {
      // For list, we don't fully populate `resources` or `labels` nested arrays for performance.
      // These fields are only fully populated on `get` or if explicitly requested.
      // The `LabelFE` type in Rust shows `resources: any[]` and `labels: string[]` (for LabelValue).
      // We will only store the IDs/values directly from the DB.
      let currentResourceLabels: string[] = [];
      let currentNestedLabels: string[] = [];

      try {
        if (rawLabel.resources)
          currentResourceLabels = JSON.parse(rawLabel.resources);
      } catch (e) {
        request.log.warn(
          `Failed to parse resources for label ${rawLabel.id}: ${e}`
        );
      }
      try {
        if (rawLabel.labels) currentNestedLabels = JSON.parse(rawLabel.labels);
      } catch (e) {
        request.log.warn(
          `Failed to parse nested labels for label ${rawLabel.id}: ${e}`
        );
      }

      const label: Label = {
        id: rawLabel.id as LabelID,
        value: rawLabel.value as LabelValue,
        public_note: rawLabel.public_note,
        private_note: rawLabel.private_note,
        color: rawLabel.color,
        created_by: rawLabel.created_by_user_id as UserID,
        created_at: rawLabel.created_at,
        last_updated_at: rawLabel.last_updated_at,
        resources: currentResourceLabels, // Populated from DB string
        labels: currentNestedLabels, // Populated from DB string
        external_id: rawLabel.external_id,
        external_payload: rawLabel.external_payload,
      };

      // Check resource-level permissions if not owner and no table-level view
      let canViewLabel = hasTablePermission || isOwner; // Already checked table permission
      if (!canViewLabel) {
        // If table-level permission is not sufficient
        const resourcePermissions = await checkSystemPermissions(
          org_id,
          `${IDPrefixEnum.LabelID}${label.id.split("_")[1]}`, // SystemRecordIDEnum::Label in Rust
          requesterApiKey.user_id,
          label.value
        );
        canViewLabel = resourcePermissions.includes(SystemPermissionType.VIEW);
      }

      if (canViewLabel) {
        labels.push(label);
      }
    }

    // Determine total count
    // The Rust implementation used `total_filtered_count` or `paginated_labels.len() + 1`.
    // For SQLite, a separate COUNT query is most accurate for `total`.
    const countQuery = `SELECT COUNT(*) AS count FROM labels ${
      conditions.length > 0 ? "WHERE " + conditions.join(" AND ") : ""
    }`;
    const countParams = params.filter((_, idx) => idx < conditions.length); // Only use filter params, not cursor params for total count
    const totalResult = await db.queryDrive(org_id, countQuery, countParams);
    const totalCount = totalResult.length > 0 ? totalResult[0].count : 0;

    // Apply redaction to the final list of labels
    const paginatedLabelsFE: LabelFE[] = await Promise.all(
      labels.map((label) =>
        castLabelToLabelFE(label, requesterApiKey.user_id, org_id)
      )
    );

    // Calculate previous cursor for bi-directional pagination (TODO for full implementation)
    // For now, if cursor_up was provided, previousCursor is null.
    // Full bi-directional cursor logic would be more complex, needing to store the `created_at` of the first item
    // and querying in reverse order.
    previousCursor = requestBody.cursor || null;

    return reply.status(200).send(
      createApiResponse<IPaginatedResponse<LabelFE>>({
        items: paginatedLabelsFE,
        page_size: pageSize,
        total: totalCount,
        cursor: nextCursor,
      })
    );
  } catch (error) {
    request.log.error("Error in listLabelsHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function createLabelHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestCreateLabel }>,
  reply: FastifyReply
): Promise<void> {
  const { org_id } = request.params;
  try {
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    const createReq = request.body;

    const validationErrors: string[] = [];

    // Validate label value
    const labelValueValidation = validateLabelValue(createReq.value);
    if (!labelValueValidation.valid || !labelValueValidation.validatedValue) {
      validationErrors.push(
        labelValueValidation.error || "Invalid label value"
      );
    }
    const validatedLabelValue = labelValueValidation.validatedValue;

    // Validate public_note
    if (createReq.public_note !== undefined) {
      const validation = validateDescription(
        createReq.public_note,
        "public_note"
      );
      if (!validation.valid) validationErrors.push(validation.error!);
    }
    // Validate private_note
    if (createReq.private_note !== undefined) {
      const validation = validateDescription(
        createReq.private_note,
        "private_note"
      );
      if (!validation.valid) validationErrors.push(validation.error!);
    }
    // Validate color
    let validatedColor: string = "#3B82F6"; // Default blue color from Rust
    if (createReq.color !== undefined) {
      const colorValidation = validateColor(createReq.color);
      if (!colorValidation.valid || !colorValidation.validatedColor) {
        validationErrors.push(colorValidation.error || "Invalid color format");
      } else {
        validatedColor = colorValidation.validatedColor;
      }
    }
    // Validate external_id
    if (createReq.external_id !== undefined) {
      const validation = validateExternalId(createReq.external_id);
      if (!validation.valid) validationErrors.push(validation.error!);
    }
    // Validate external_payload
    if (createReq.external_payload !== undefined) {
      const validation = validateExternalPayload(createReq.external_payload);
      if (!validation.valid) validationErrors.push(validation.error!);
    }

    if (validationErrors.length > 0) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validationErrors.join("; "),
        })
      );
    }

    // Check create permission if not owner
    if (!isOwner) {
      const tablePermissions = await checkSystemPermissions(
        org_id,
        `LABELS`, // SystemTableEnum::Labels in Rust
        requesterApiKey.user_id
      );

      if (!tablePermissions.includes(SystemPermissionType.CREATE)) {
        return reply
          .status(403)
          .send(
            createApiResponse(undefined, { code: 403, message: "Forbidden" })
          );
      }
    }

    // Check if label already exists by value
    const existingLabelByValue = await db.queryDrive(
      org_id,
      "SELECT id FROM labels WHERE value = ?",
      [validatedLabelValue]
    );

    if (existingLabelByValue.length > 0) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: `Label '${createReq.value}' already exists`,
        })
      );
    }

    const current_time = Date.now(); // Milliseconds
    const labelId = (createReq.id ||
      `${IDPrefixEnum.LabelID}${uuidv4()}`) as LabelID;

    // Build the new label object
    const newLabel: Label = {
      id: labelId,
      value: validatedLabelValue as LabelValue,
      public_note: createReq.public_note || "",
      private_note: createReq.private_note || "",
      color: validatedColor,
      created_by: requesterApiKey.user_id,
      created_at: current_time,
      last_updated_at: current_time,
      resources: [], // New labels start with no resources
      labels: [], // New labels start with no nested labels
      external_id: createReq.external_id || undefined,
      external_payload: createReq.external_payload || undefined,
    };

    // Store the label in the database within a transaction
    await dbHelpers.transaction("drive", org_id, (database) => {
      database
        .prepare(
          `INSERT INTO labels (id, value, public_note, private_note, color, created_by_user_id, created_at, last_updated_at, resources, labels, external_id, external_payload)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
        )
        .run(
          newLabel.id,
          newLabel.value,
          newLabel.public_note,
          newLabel.private_note,
          newLabel.color,
          newLabel.created_by,
          newLabel.created_at,
          newLabel.last_updated_at,
          JSON.stringify(newLabel.resources), // Store array as JSON string
          JSON.stringify(newLabel.labels), // Store array as JSON string
          newLabel.external_id,
          newLabel.external_payload
        );
      request.log.debug(`Created label ${newLabel.id}`);
    });

    // TODO: DRIVE update_external_id_mapping (Rust had this)
    await updateExternalIdMapping(
      null,
      newLabel.external_id,
      newLabel.id,
      org_id
    );

    const labelFE = await castLabelToLabelFE(
      newLabel,
      requesterApiKey.user_id,
      org_id
    );

    return reply.status(200).send(createApiResponse<LabelFE>(labelFE));
  } catch (error) {
    request.log.error("Error in createLabelHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function updateLabelHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestUpdateLabel }>,
  reply: FastifyReply
): Promise<void> {
  const { org_id } = request.params;
  try {
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    const updateReq = request.body;

    const validationErrors: string[] = [];
    // Validate label ID
    const idValidation = validateIdString(updateReq.id, "id");
    if (!idValidation.valid) validationErrors.push(idValidation.error!);

    // Validate optional fields if provided
    if (updateReq.value !== undefined) {
      const validation = validateLabelValue(updateReq.value);
      if (!validation.valid) validationErrors.push(validation.error!);
    }
    if (updateReq.description !== undefined) {
      const validation = validateDescription(
        updateReq.description,
        "description"
      );
      if (!validation.valid) validationErrors.push(validation.error!);
    }
    if (updateReq.color !== undefined) {
      const validation = validateColor(updateReq.color);
      if (!validation.valid) validationErrors.push(validation.error!);
    }
    if (updateReq.external_id !== undefined) {
      const validation = validateExternalId(updateReq.external_id);
      if (!validation.valid) validationErrors.push(validation.error!);
    }
    if (updateReq.external_payload !== undefined) {
      const validation = validateExternalPayload(updateReq.external_payload);
      if (!validation.valid) validationErrors.push(validation.error!);
    }

    if (validationErrors.length > 0) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validationErrors.join("; "),
        })
      );
    }

    const labelId = updateReq.id as LabelID;

    // Get existing label
    const existingLabels = await db.queryDrive(
      org_id,
      "SELECT id, value, public_note, private_note, color, created_by_user_id, created_at, last_updated_at, external_id, external_payload, resources, labels FROM labels WHERE id = ?",
      [labelId]
    );

    if (!existingLabels || existingLabels.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Label not found",
        })
      );
    }

    let existingLabel = existingLabels[0] as Label; // Cast to Label type

    // Check update permission if not owner
    if (!isOwner) {
      const tablePermissions = await checkSystemPermissions(
        org_id,
        `LABELS`, // SystemTableEnum::Labels in Rust
        requesterApiKey.user_id,
        existingLabel.value
      );

      const resourcePermissions = await checkSystemPermissions(
        org_id,
        `${IDPrefixEnum.LabelID}${labelId.split("_")[1]}`, // SystemRecordIDEnum::Label in Rust
        requesterApiKey.user_id,
        existingLabel.value
      );

      if (
        !tablePermissions.includes(SystemPermissionType.EDIT) &&
        !resourcePermissions.includes(SystemPermissionType.EDIT)
      ) {
        return reply
          .status(403)
          .send(
            createApiResponse(undefined, { code: 403, message: "Forbidden" })
          );
      }
    }

    // Store old external_id for mapping cleanup
    const oldExternalId = existingLabel.external_id;

    // Update fields and track changes for SQL update statement
    const updates: string[] = [];
    const params: any[] = [];

    if (updateReq.value !== undefined) {
      const validatedValue = validateLabelValue(updateReq.value).validatedValue;
      if (validatedValue && validatedValue !== existingLabel.value) {
        // Rust's `update_label_string_value` is complex and updates all resources.
        // In SQL, we'd need to update the `value` in the `labels` table directly,
        // and potentially update `label_id` references in junction tables if `value` was the reference.
        // Given that `label_id` is the FK and `value` is unique, changing `value` on the `labels` table is sufficient.
        // However, if other tables store `LabelValue` directly instead of `LabelID`, those would need updating.
        // Based on the DDL, only the `labels` table stores `value` as UNIQUE,
        // other junction tables use `label_id`.
        // So, we update `value` directly here.
        updates.push("value = ?");
        params.push(validatedValue);
        existingLabel.value = validatedValue; // Update in memory for `castLabelToLabelFE`
      }
    }
    if (updateReq.description !== undefined) {
      updates.push("public_note = ?"); // Assuming description maps to public_note
      params.push(updateReq.description);
      existingLabel.public_note = updateReq.description;
    }
    // private_note is not directly updatable via IRequestUpdateLabel in types.ts.
    // If it were, you'd add:
    // if (updateReq.private_note !== undefined) {
    //   updates.push("private_note = ?");
    //   params.push(updateReq.private_note);
    //   existingLabel.private_note = updateReq.private_note;
    // }
    if (updateReq.color !== undefined) {
      const validatedColor = validateColor(updateReq.color).validatedColor;
      if (validatedColor) {
        updates.push("color = ?");
        params.push(validatedColor);
        existingLabel.color = validatedColor;
      }
    }
    if (updateReq.external_id !== undefined) {
      updates.push("external_id = ?");
      params.push(updateReq.external_id);
      existingLabel.external_id = updateReq.external_id;
      // Handle external ID mapping change
      await updateExternalIdMapping(
        oldExternalId,
        updateReq.external_id,
        labelId,
        org_id
      );
    }
    if (updateReq.external_payload !== undefined) {
      updates.push("external_payload = ?");
      params.push(updateReq.external_payload);
      existingLabel.external_payload = updateReq.external_payload;
    }

    if (updates.length === 0) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "No fields to update",
        })
      );
    }

    // Update last_updated_at
    updates.push("last_updated_at = ?");
    params.push(Date.now());

    // Execute update within a transaction
    await dbHelpers.transaction("drive", org_id, (database) => {
      database
        .prepare(`UPDATE labels SET ${updates.join(", ")} WHERE id = ?`)
        .run(...params, labelId);
      request.log.debug(`Updated label ${labelId}`);
    });

    // Re-fetch the updated label to ensure all derived fields are current
    const updatedLabelResult = await db.queryDrive(
      org_id,
      "SELECT id, value, public_note, private_note, color, created_by_user_id, created_at, last_updated_at, external_id, external_payload, resources, labels FROM labels WHERE id = ?",
      [labelId]
    );

    if (!updatedLabelResult || updatedLabelResult.length === 0) {
      // This should ideally not happen if the update succeeded
      throw new Error("Failed to retrieve updated label.");
    }
    const updatedRawLabel = updatedLabelResult[0];

    // Reconstruct Label object from DB result for FE casting
    // Note: `resources` and `labels` fields are JSON strings in DB, need parsing
    let updatedLabelResources: string[] = [];
    let updatedNestedLabels: string[] = [];
    try {
      if (updatedRawLabel.resources)
        updatedLabelResources = JSON.parse(updatedRawLabel.resources);
    } catch (e) {
      request.log.warn(
        `Failed to parse resources for updated label ${updatedRawLabel.id}: ${e}`
      );
    }
    try {
      if (updatedRawLabel.labels)
        updatedNestedLabels = JSON.parse(updatedRawLabel.labels);
    } catch (e) {
      request.log.warn(
        `Failed to parse nested labels for updated label ${updatedRawLabel.id}: ${e}`
      );
    }

    const finalUpdatedLabel: Label = {
      id: updatedRawLabel.id as LabelID,
      value: updatedRawLabel.value as LabelValue,
      public_note: updatedRawLabel.public_note,
      private_note: updatedRawLabel.private_note,
      color: updatedRawLabel.color,
      created_by: updatedRawLabel.created_by_user_id as UserID,
      created_at: updatedRawLabel.created_at,
      last_updated_at: updatedRawLabel.last_updated_at,
      resources: updatedLabelResources,
      labels: updatedNestedLabels,
      external_id: updatedRawLabel.external_id,
      external_payload: updatedRawLabel.external_payload,
    };

    const labelFE = await castLabelToLabelFE(
      finalUpdatedLabel,
      requesterApiKey.user_id,
      org_id
    );

    return reply.status(200).send(createApiResponse<LabelFE>(labelFE));
  } catch (error) {
    request.log.error("Error in updateLabelHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function deleteLabelHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestDeleteLabel }>,
  reply: FastifyReply
): Promise<void> {
  const { org_id } = request.params;

  try {
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const body = request.body;

    // Validate request body
    const validation = validateIdString(body.id, "id");
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    const labelId = body.id as LabelID;

    // Get existing label
    const existingLabels = await db.queryDrive(
      org_id,
      "SELECT id, value, external_id FROM labels WHERE id = ?",
      [labelId]
    );

    if (!existingLabels || existingLabels.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Label not found",
        })
      );
    }

    const labelToDelete = existingLabels[0];
    const labelValue = labelToDelete.value as LabelValue;
    const oldExternalId = labelToDelete.external_id;
    const oldInternalId = labelToDelete.id;

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    // Check delete permission if not owner
    if (!isOwner) {
      const tablePermissions = await checkSystemPermissions(
        org_id,
        `LABELS`, // SystemTableEnum::Labels in Rust
        requesterApiKey.user_id,
        labelValue
      );

      const resourcePermissions = await checkSystemPermissions(
        org_id,
        `${IDPrefixEnum.LabelID}${labelId.split("_")[1]}`, // SystemRecordIDEnum::Label in Rust
        requesterApiKey.user_id,
        labelValue
      );

      if (
        !tablePermissions.includes(SystemPermissionType.DELETE) &&
        !resourcePermissions.includes(SystemPermissionType.DELETE)
      ) {
        return reply
          .status(403)
          .send(
            createApiResponse(undefined, { code: 403, message: "Forbidden" })
          );
      }
    }

    // Perform delete operation within a transaction
    await dbHelpers.transaction("drive", org_id, (database) => {
      // 1. Remove label associations from all junction tables
      const junctionTables = [
        "api_key_labels",
        "contact_labels",
        "file_labels",
        "folder_labels",
        "disk_labels",
        "drive_labels",
        "group_labels",
        "group_invite_labels",
        "permission_directory_labels",
        "permission_system_labels",
        "webhook_labels",
        "label_labels", // Both parent and child label IDs might need to be removed if this label is part of a hierarchy
      ];

      for (const tableName of junctionTables) {
        let stmtSql = "";
        // For label_labels, need to handle both parent_label_id and child_label_id
        if (tableName === "label_labels") {
          stmtSql = `DELETE FROM ${tableName} WHERE parent_label_id = ? OR child_label_id = ?`;
          database.prepare(stmtSql).run(labelId, labelId);
        } else {
          // All other tables use 'label_id'
          stmtSql = `DELETE FROM ${tableName} WHERE label_id = ?`;
          database.prepare(stmtSql).run(labelId);
        }
        request.log.debug(`Deleted from ${tableName} for label ${labelId}`);
      }

      // 2. Delete the label itself
      const deleteLabelStmt = database.prepare(
        "DELETE FROM labels WHERE id = ?"
      );
      deleteLabelStmt.run(labelId);
      request.log.debug(`Deleted label ${labelId} from labels table.`);
    });

    // TODO: DRIVE Update external ID mapping (Rust had this as `update_external_id_mapping(old_external_id, None, old_internal_id)`)
    await updateExternalIdMapping(oldExternalId, null, oldInternalId, org_id);

    return reply.status(200).send(
      createApiResponse<IResponseDeleteLabel["ok"]["data"]>({
        id: labelId,
        deleted: true,
      })
    );
  } catch (error) {
    request.log.error("Error in deleteLabelHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

export async function labelResourceHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestLabelResource }>,
  reply: FastifyReply
): Promise<void> {
  const { org_id } = request.params;

  try {
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const body = request.body;

    // Validate request body
    const validationErrors: string[] = [];
    let validation = validateIdString(body.label_id, "label_id");
    if (!validation.valid) validationErrors.push(validation.error!);
    validation = validateIdString(body.resource_id, "resource_id");
    if (!validation.valid) validationErrors.push(validation.error!);

    if (validationErrors.length > 0) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validationErrors.join("; "),
        })
      );
    }

    const labelId = body.label_id as LabelID;
    const resourceIdString = body.resource_id;
    const addOperation = body.add;

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    // Get the label to access its value and properties
    const existingLabels = await db.queryDrive(
      org_id,
      "SELECT id, value, public_note, private_note, color, created_by_user_id, created_at, last_updated_at, external_id, external_payload, resources, labels FROM labels WHERE id = ?",
      [labelId]
    );

    if (!existingLabels || existingLabels.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: `Label with ID ${labelId} not found`,
        })
      );
    }

    const rawLabel = existingLabels[0];
    const labelValue = rawLabel.value as LabelValue;

    // Parse resource ID and determine its type
    const parsedResource = parseLabelResourceID(resourceIdString);
    if (
      !parsedResource.valid ||
      !parsedResource.resourceId ||
      !parsedResource.resourceTypePrefix
    ) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: parsedResource.error!,
        })
      );
    }
    const actualResourceId = parsedResource.resourceId; // This is the full ID string, e.g., "FileID_xyz"
    const resourceTypePrefix = parsedResource.resourceTypePrefix; // e.g., "FileID" or "FolderID"

    // Check permissions if not owner
    if (!isOwner) {
      // Check table-level permissions for 'Labels'
      const tablePermissions = await checkSystemPermissions(
        org_id,
        `LABELS`, // SystemTableEnum::Labels in Rust
        requesterApiKey.user_id,
        labelValue
      );

      // Check permissions for the specific resource being labeled/unlabeled
      const resourceBeingLabeledPermissions = await checkSystemPermissions(
        org_id,
        `${resourceTypePrefix}_${actualResourceId.split("_").slice(1).join("_")}`, // Reconstruct SystemResourceID
        requesterApiKey.user_id
      );

      if (
        !(
          tablePermissions.includes(SystemPermissionType.EDIT) ||
          resourceBeingLabeledPermissions.includes(SystemPermissionType.EDIT)
        )
      ) {
        return reply
          .status(403)
          .send(
            createApiResponse(undefined, { code: 403, message: "Forbidden" })
          );
      }
    }

    // Start a transaction for atomicity
    await dbHelpers.transaction("drive", org_id, async (database) => {
      let junctionTableName = "";
      let resourceLabelsColumn = "labels"; // The column name for labels on the resource table (all are 'labels')
      let junctionTableResourceIdColumn = ""; // The column name for the resource ID in the junction table

      // Determine table name and column for the resource
      let resourceTableName: string;
      switch (resourceTypePrefix) {
        case IDPrefixEnum.ApiKey.slice(0, -1):
          resourceTableName = "api_keys";
          junctionTableName = "api_key_labels";
          junctionTableResourceIdColumn = "api_key_id";
          break;
        case IDPrefixEnum.User.slice(0, -1):
          resourceTableName = "contacts";
          junctionTableName = "contact_labels";
          junctionTableResourceIdColumn = "user_id";
          break;
        case IDPrefixEnum.File.slice(0, -1):
          resourceTableName = "files";
          junctionTableName = "file_labels";
          junctionTableResourceIdColumn = "file_id";
          break;
        case IDPrefixEnum.Folder.slice(0, -1):
          resourceTableName = "folders";
          junctionTableName = "folder_labels";
          junctionTableResourceIdColumn = "folder_id";
          break;
        case IDPrefixEnum.Disk.slice(0, -1):
          resourceTableName = "disks";
          junctionTableName = "disk_labels";
          junctionTableResourceIdColumn = "disk_id";
          break;
        case IDPrefixEnum.Drive.slice(0, -1):
          resourceTableName = "drives";
          junctionTableName = "drive_labels";
          junctionTableResourceIdColumn = "drive_id";
          break;
        case IDPrefixEnum.DirectoryPermission.slice(0, -1):
          resourceTableName = "permissions_directory";
          junctionTableName = "permission_directory_labels";
          junctionTableResourceIdColumn = "permission_id";
          break;
        case IDPrefixEnum.SystemPermission.slice(0, -1):
          resourceTableName = "permissions_system";
          junctionTableName = "permission_system_labels";
          junctionTableResourceIdColumn = "permission_id";
          break;
        case IDPrefixEnum.GroupInvite.slice(0, -1):
          resourceTableName = "group_invites";
          junctionTableName = "group_invite_labels";
          junctionTableResourceIdColumn = "invite_id";
          break;
        case IDPrefixEnum.Group.slice(0, -1):
          resourceTableName = "groups";
          junctionTableName = "group_labels";
          junctionTableResourceIdColumn = "group_id";
          break;
        case IDPrefixEnum.Webhook.slice(0, -1):
          resourceTableName = "webhooks";
          junctionTableName = "webhook_labels";
          junctionTableResourceIdColumn = "webhook_id";
          break;
        case IDPrefixEnum.LabelID.slice(0, -1): // Label can be labeled itself
          resourceTableName = "labels";
          junctionTableName = "label_labels";
          junctionTableResourceIdColumn = "parent_label_id"; // When a label is being labeled, it's the parent
          break;
        default:
          throw new Error(`Unsupported resource type: ${resourceTypePrefix}`);
      }

      // 1. Update the resource's `labels` column (stored as JSON string in SQLite)
      const existingResource = database
        .prepare(
          `SELECT ${resourceLabelsColumn} FROM ${resourceTableName} WHERE id = ?`
        )
        .get(actualResourceId) as unknown as Record<string, any>;
      let currentResourceLabels: string[] = [];
      if (existingResource && existingResource[resourceLabelsColumn]) {
        try {
          currentResourceLabels = JSON.parse(
            existingResource[resourceLabelsColumn]
          );
        } catch (e) {
          request.log.warn(
            `Failed to parse labels for resource ${actualResourceId}: ${e}. Assuming empty array.`
          );
          currentResourceLabels = [];
        }
      }

      let updatedResourceLabels = [...currentResourceLabels];
      let junctionTableActionSql = "";
      let junctionTableParams: any[] = [];

      if (addOperation) {
        if (!updatedResourceLabels.includes(labelValue)) {
          updatedResourceLabels.push(labelValue);
          // Insert into junction table
          if (junctionTableName === "label_labels") {
            junctionTableActionSql = `INSERT INTO ${junctionTableName} (${junctionTableResourceIdColumn}, child_label_id) VALUES (?, ?)`;
            junctionTableParams = [actualResourceId, labelId]; // parent_label_id = resource_id, child_label_id = label_id
          } else {
            junctionTableActionSql = `INSERT INTO ${junctionTableName} (${junctionTableResourceIdColumn}, label_id) VALUES (?, ?)`;
            junctionTableParams = [actualResourceId, labelId];
          }
          database.prepare(junctionTableActionSql).run(...junctionTableParams);
        }
      } else {
        // Remove operation
        const index = updatedResourceLabels.indexOf(labelValue);
        if (index > -1) {
          updatedResourceLabels.splice(index, 1);
          // Delete from junction table
          if (junctionTableName === "label_labels") {
            junctionTableActionSql = `DELETE FROM ${junctionTableName} WHERE ${junctionTableResourceIdColumn} = ? AND child_label_id = ?`;
            junctionTableParams = [actualResourceId, labelId];
          } else {
            junctionTableActionSql = `DELETE FROM ${junctionTableName} WHERE ${junctionTableResourceIdColumn} = ? AND label_id = ?`;
            junctionTableParams = [actualResourceId, labelId];
          }
          database.prepare(junctionTableActionSql).run(...junctionTableParams);
        }
      }

      // Update the resource's labels column
      let updateResourceSql = `UPDATE ${resourceTableName} SET ${resourceLabelsColumn} = ?`;
      const updateResourceParams: any[] = [
        JSON.stringify(updatedResourceLabels),
      ];

      // Add `last_updated_at` or `last_modified_at` for relevant tables
      const hasTimestamp = [
        "files",
        "folders",
        "permissions_directory",
        "permissions_system",
        "group_invites",
        "groups",
        "webhooks",
        "labels",
      ].includes(resourceTableName);
      if (hasTimestamp) {
        updateResourceSql += `, ${resourceTableName.includes("permissions") || resourceTableName.includes("group") ? "last_modified_at" : "last_updated_at"} = ?`;
        updateResourceParams.push(Date.now());
      }
      updateResourceSql += ` WHERE id = ?`;
      updateResourceParams.push(actualResourceId);

      database.prepare(updateResourceSql).run(...updateResourceParams);
      request.log.debug(
        `${addOperation ? "Added" : "Removed"} label ${labelValue} to resource ${actualResourceId}`
      );

      // 2. Update the label's `resources` array on the Label object itself
      // This is the `resources: Vec<LabelResourceID>` field on the Label struct.
      // This is stored in the `labels` table directly in a JSON column.
      let currentLabelResources: string[] = [];
      try {
        if (rawLabel.resources) {
          currentLabelResources = JSON.parse(rawLabel.resources);
        }
      } catch (e) {
        request.log.warn(
          `Failed to parse resources for label ${labelId}: ${e}. Assuming empty array.`
        );
        currentLabelResources = [];
      }

      let updatedLabelResources = [...currentLabelResources];
      if (addOperation) {
        if (!updatedLabelResources.includes(actualResourceId)) {
          updatedLabelResources.push(actualResourceId);
        }
      } else {
        // Remove operation
        const index = updatedLabelResources.indexOf(actualResourceId);
        if (index > -1) {
          updatedLabelResources.splice(index, 1);
        }
        // If label's resources become empty after removal, Rust code deletes the label entirely.
        if (updatedLabelResources.length === 0) {
          request.log.debug(
            `Label '${labelValue}' no longer has any resources. Deleting label.`
          );
          database.prepare("DELETE FROM labels WHERE id = ?").run(labelId);
          // Also delete associated nested label relationships where this label is a child
          database
            .prepare(`DELETE FROM label_labels WHERE child_label_id = ?`)
            .run(labelId);
          await updateExternalIdMapping(
            rawLabel.external_id,
            null,
            labelId,
            org_id
          ); // Clean up external ID mapping
        }
      }

      // Only update the label's `resources` and `last_updated_at` if the label itself still exists
      if (updatedLabelResources.length > 0 || addOperation) {
        // If adding, or if not empty after removing
        // Make sure the label still exists before attempting to update it
        const labelExists = database
          .prepare("SELECT id FROM labels WHERE id = ?")
          .get(labelId);
        if (labelExists) {
          database
            .prepare(
              "UPDATE labels SET resources = ?, last_updated_at = ? WHERE id = ?"
            )
            .run(JSON.stringify(updatedLabelResources), Date.now(), labelId);
        }
      }
    });

    const actionMessage = addOperation ? "added" : "removed";

    // Fire webhook if needed
    const webhookEvent = addOperation
      ? WebhookEventLabel.LABEL_ADDED
      : WebhookEventLabel.LABEL_REMOVED;
    const webhooks = await getActiveLabelWebhooks(
      labelId,
      webhookEvent,
      org_id
    );
    if (webhooks.length > 0) {
      const notes = `Label ${labelId} ${actionMessage} resource ${actualResourceId}`;
      // For LabelWebhookData, we need the full Label object. Re-fetch it after modification.
      // Or pass relevant data directly. The Rust `LabelWebhookData` has specific fields.
      const webhookPayloadData = {
        label_id: labelId,
        resource_id: actualResourceId,
        label_value: labelValue,
        add: addOperation,
      };
      await fireLabelWebhook(
        webhookEvent,
        webhooks,
        null, // No pre-state snapshot for webhooks here
        webhookPayloadData, // Simple data for webhook payload
        notes,
        org_id
      );
    }

    // Re-fetch the label to return its updated state (if it still exists)
    let finalLabel: Label | undefined;
    const finalLabelQuery = await db.queryDrive(
      org_id,
      "SELECT id, value, public_note, private_note, color, created_by_user_id, created_at, last_updated_at, external_id, external_payload, resources, labels FROM labels WHERE id = ?",
      [labelId]
    );

    if (finalLabelQuery.length > 0) {
      const rawFinalLabel = finalLabelQuery[0];
      let finalLabelResources: string[] = [];
      let finalNestedLabels: string[] = [];
      try {
        if (rawFinalLabel.resources)
          finalLabelResources = JSON.parse(rawFinalLabel.resources);
      } catch (e) {
        /* ignore parse error for mock */
      }
      try {
        if (rawFinalLabel.labels)
          finalNestedLabels = JSON.parse(rawFinalLabel.labels);
      } catch (e) {
        /* ignore parse error for mock */
      }

      finalLabel = {
        id: rawFinalLabel.id as LabelID,
        value: rawFinalLabel.value as LabelValue,
        public_note: rawFinalLabel.public_note,
        private_note: rawFinalLabel.private_note,
        color: rawFinalLabel.color,
        created_by: rawFinalLabel.created_by_user_id as UserID,
        created_at: rawFinalLabel.created_at,
        last_updated_at: rawFinalLabel.last_updated_at,
        resources: finalLabelResources,
        labels: finalNestedLabels,
        external_id: rawFinalLabel.external_id,
        external_payload: rawFinalLabel.external_payload,
      } as Label;
    }

    return reply.status(200).send(
      createApiResponse<IResponseLabelResource["ok"]["data"]>({
        success: true,
        message: `Successfully ${actionMessage}ed label.`,
        label: finalLabel
          ? await castLabelToLabelFE(
              finalLabel,
              requesterApiKey.user_id,
              org_id
            )
          : undefined, // Cast to FE type
      })
    );
  } catch (error) {
    request.log.error("Error in labelResourceHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: "Internal server error",
      })
    );
  }
}

// New helper function: `redactLabelValue`
export async function redactLabelValue(
  orgId: DriveID,
  labelValue: LabelValue,
  requesterUserId: UserID
): Promise<LabelValue | null> {
  // 1. Get the LabelID from the value. In Rust, this is `LABELS_BY_VALUE_HASHTABLE`.
  // In SQLite, we need a DB lookup.
  const labelResult = await db.queryDrive(
    orgId,
    "SELECT id FROM labels WHERE value = ?",
    [labelValue]
  );

  if (labelResult.length === 0) {
    return null; // Label not found, effectively redacted
  }
  const labelId: LabelID = labelResult[0].id;

  // 2. Check if the user is the owner (Owner bypass)
  const isOwner = requesterUserId === (await getDriveOwnerId(orgId));
  if (isOwner) {
    return labelValue; // Owner sees everything
  }

  // 3. Check permissions for this specific label and the Labels table
  // Rust uses `check_system_resource_permissions_labels` which handles label prefixes.
  // Your `checkSystemPermissions` mock needs to correctly replicate this.
  const recordPermissions = await checkSystemPermissions(
    orgId,
    `${IDPrefixEnum.LabelID}${labelId.split("_")[1]}`, // Reconstruct SystemResourceID for label record
    requesterUserId,
    labelValue // Pass labelValue for label-prefixed permission checks
  );

  const tablePermissions = await checkSystemPermissions(
    orgId,
    `LABELS`, // SystemTableEnum::Labels in Rust
    requesterUserId,
    labelValue // Pass labelValue for label-prefixed permission checks
  );

  // If the user has View permission either at the table level or for this specific label
  if (
    recordPermissions.includes(SystemPermissionType.VIEW) ||
    tablePermissions.includes(SystemPermissionType.VIEW)
  ) {
    return labelValue;
  }

  // If we get here, the user doesn't have permission to see this label
  return null;
}

// Update `castLabelToLabelFE` to use `redactLabelValue`
async function castLabelToLabelFE(
  label: Label,
  requesterUserId: UserID,
  orgId: DriveID
): Promise<LabelFE> {
  const permissionPreviews: SystemPermissionType[] = []; // TODO: PERMIT Populate with actual permissions (similar to getLabelHandler)

  const isOwner = requesterUserId === (await getDriveOwnerId(orgId));

  let privateNote: string | null | undefined = label.private_note;
  let resources: any[] = label.resources;
  let labels: LabelValue[] = label.labels; // Labels applied to this label

  if (!isOwner) {
    resources = []; // Non-owners don't see what resources a label is applied to.

    // Check for EDIT permission on the label itself to show private_note
    const hasEditPermission = (
      await checkSystemPermissions(
        orgId,
        `${IDPrefixEnum.LabelID}${label.id.split("_")[1]}`,
        requesterUserId,
        label.value // Pass label value for prefix permissions
      )
    ).includes(SystemPermissionType.EDIT);
    if (!hasEditPermission) {
      privateNote = undefined;
    }

    // Filter nested labels based on user's permissions using redactLabelValue
    const redactedNestedLabels: LabelValue[] = [];
    for (const nestedLabelValue of label.labels) {
      const redacted = await redactLabelValue(
        orgId,
        nestedLabelValue,
        requesterUserId
      );
      if (redacted !== null) {
        redactedNestedLabels.push(redacted);
      }
    }
    labels = redactedNestedLabels;
  }

  // --- Populate permission_previews based on existing logic in getLabelHandler ---
  const tablePermissions = await checkSystemPermissions(
    orgId,
    `LABELS`, // SystemTableEnum::Labels in Rust
    requesterUserId,
    label.value // For label-prefixed permissions
  );

  const resourcePermissions = await checkSystemPermissions(
    orgId,
    `${IDPrefixEnum.LabelID}${label.id.split("_")[1]}`, // SystemRecordIDEnum::Label in Rust
    requesterUserId,
    label.value
  );

  // Combine and deduplicate permissions
  const combinedPermissions = [
    ...new Set([...tablePermissions, ...resourcePermissions]),
  ];
  // Ensure 'permission_previews' contains only VIEW if no other permissions are explicitly granted and not owner
  // Rust's `Label::cast_fe` combines record and table permissions, then passes them to `redacted(user_id)`
  // which implies permission_previews are calculated *before* redaction for the main label fields.
  // The specific fields like `private_note`, `resources`, `labels` are then redacted by `redacted(user_id)`
  // based on these permissions.
  // For simplicity, we'll populate `permission_previews` here with what the user *has* for this label.
  if (isOwner) {
    permissionPreviews.push(
      SystemPermissionType.CREATE,
      SystemPermissionType.EDIT,
      SystemPermissionType.DELETE,
      SystemPermissionType.VIEW,
      SystemPermissionType.INVITE
    );
  } else {
    // If not owner, only show permissions relevant to non-owners, which is what they actually have.
    // This is already what `combinedPermissions` holds.
    permissionPreviews.push(...combinedPermissions);
  }
  // --- End permission_previews population ---

  return {
    ...label,
    private_note: privateNote,
    resources: resources,
    labels: labels,
    permission_previews: permissionPreviews,
  };
}
