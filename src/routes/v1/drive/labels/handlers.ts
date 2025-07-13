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
  SystemResourceID,
  SystemTableValueEnum,
  LabelWebhookData, // Import LabelWebhookData for type safety
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import { authenticateRequest } from "../../../../services/auth";
import { getDriveOwnerId, OrgIdParams } from "../../types";
// Import the actual permission checking functions
import {
  checkSystemPermissions,
  checkSystemResourcePermissionsLabels,
  redactLabelValue,
} from "../../../../services/permissions/system";
import {
  validateColor,
  validateDescription,
  validateExternalId,
  validateExternalPayload,
  validateIdString,
  validateLabelValue,
  validateShortString,
} from "../../../../services/validation";

// Import the actual webhook service functions
import {
  getActiveLabelWebhooks,
  fireLabelWebhook,
} from "../../../../services/webhooks";
import {
  claimUUID,
  isUUIDClaimed,
  updateExternalIDMapping,
} from "../../../../services/external";

interface GetLabelParams extends OrgIdParams {
  label_id: string; // Can be LabelID or LabelValue
}

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

// From Rust `parse_label_resource_id`
function parseLabelResourceID(idStr: string): {
  valid: boolean;
  resourceId?: string; // The full prefixed ID, e.g., "FileID_uuid"
  resourceTypePrefix?: string; // The prefix without the trailing underscore, e.g., "FileID"
  resourceTableName?: string; // The corresponding SQL table name, e.g., "files"
  error?: string;
} {
  // Map candidate prefixes to IDPrefixEnum values and corresponding SQL table names
  const prefixMap: { [key in IDPrefixEnum]?: string } = {
    [IDPrefixEnum.File]: "files",
    [IDPrefixEnum.Folder]: "folders",
    [IDPrefixEnum.ApiKey]: "api_keys",
    [IDPrefixEnum.User]: "contacts",
    [IDPrefixEnum.Drive]: "drives",
    [IDPrefixEnum.Disk]: "disks",
    [IDPrefixEnum.Group]: "groups",
    [IDPrefixEnum.GroupInvite]: "group_invites",
    [IDPrefixEnum.SystemPermission]: "permissions_system",
    [IDPrefixEnum.DirectoryPermission]: "permissions_directory",
    [IDPrefixEnum.Webhook]: "webhooks",
    [IDPrefixEnum.LabelID]: "labels", // Label can be labeled itself
  };

  for (const enumKey in prefixMap) {
    const prefix = enumKey as IDPrefixEnum;
    if (idStr.startsWith(prefix)) {
      const resourceTypePrefix = prefix.slice(0, -1); // Remove the trailing '_'
      return {
        valid: true,
        resourceId: idStr, // The full ID string
        resourceTypePrefix: resourceTypePrefix,
        resourceTableName: prefixMap[prefix],
      };
    }
  }

  return {
    valid: false,
    error: `Invalid resource ID prefix or format: ${idStr}`,
  };
}

async function castLabelToLabelFE(
  label: Label,
  requesterUserId: UserID,
  orgId: DriveID
): Promise<LabelFE> {
  const isOwner = requesterUserId === (await getDriveOwnerId(orgId));

  // Get user's system permissions for this label record
  const labelSystemResourceId = `${IDPrefixEnum.LabelID}${label.id.substring(IDPrefixEnum.LabelID.length)}`;

  const recordPermissions = await checkSystemPermissions(
    labelSystemResourceId,
    requesterUserId,
    orgId
  );
  const tablePermissions = await checkSystemPermissions(
    `TABLE_${SystemTableValueEnum.LABELS}`,
    requesterUserId,
    orgId
  );
  // Also include label-specific permissions
  const labelPrefixPermissions = await checkSystemResourcePermissionsLabels(
    `TABLE_${SystemTableValueEnum.LABELS}`,
    requesterUserId,
    label.value,
    orgId
  );

  const permissionPreviews: SystemPermissionType[] = Array.from(
    new Set([
      ...recordPermissions,
      ...tablePermissions,
      ...labelPrefixPermissions,
    ])
  );

  // Fetch associated resources from junction tables
  const associatedResources: string[] = [];
  const junctionTables = [
    {
      name: "api_key_labels",
      resourceIdColumn: "api_key_id",
      prefix: IDPrefixEnum.ApiKey,
    },
    {
      name: "contact_labels",
      resourceIdColumn: "user_id",
      prefix: IDPrefixEnum.User,
    },
    {
      name: "file_labels",
      resourceIdColumn: "file_id",
      prefix: IDPrefixEnum.File,
    },
    {
      name: "folder_labels",
      resourceIdColumn: "folder_id",
      prefix: IDPrefixEnum.Folder,
    },
    {
      name: "disk_labels",
      resourceIdColumn: "disk_id",
      prefix: IDPrefixEnum.Disk,
    },
    {
      name: "drive_labels",
      resourceIdColumn: "drive_id",
      prefix: IDPrefixEnum.Drive,
    },
    {
      name: "group_labels",
      resourceIdColumn: "group_id",
      prefix: IDPrefixEnum.Group,
    },
    {
      name: "group_invite_labels",
      resourceIdColumn: "invite_id",
      prefix: IDPrefixEnum.GroupInvite,
    },
    {
      name: "permission_directory_labels",
      resourceIdColumn: "permission_id",
      prefix: IDPrefixEnum.DirectoryPermission,
    },
    {
      name: "permission_system_labels",
      resourceIdColumn: "permission_id",
      prefix: IDPrefixEnum.SystemPermission,
    },
    {
      name: "webhook_labels",
      resourceIdColumn: "webhook_id",
      prefix: IDPrefixEnum.Webhook,
    },
  ];

  for (const { name: tableName, resourceIdColumn, prefix } of junctionTables) {
    const res = await db.queryDrive(
      orgId,
      `SELECT ${resourceIdColumn} FROM ${tableName} WHERE label_id = ?`,
      [label.id.substring(IDPrefixEnum.LabelID.length)] // Use plain label ID for junction table query
    );
    res.forEach((row: any) => {
      if (row[resourceIdColumn]) {
        // Construct the full prefixed ID string to match LabelResourceID type
        const fullResourceId = `${prefix}${row[resourceIdColumn]}`;
        associatedResources.push(fullResourceId);
      }
    });
  }

  // Fetch nested labels (labels applied to this label) from label_labels junction table
  const nestedLabels: LabelValue[] = []; // Stores LabelValue (string)
  const nestedLabelResults = await db.queryDrive(
    orgId,
    `SELECT T2.value FROM label_labels AS T1 JOIN labels AS T2 ON T1.child_label_id = T2.id WHERE T1.parent_label_id = ?`,
    [label.id.substring(IDPrefixEnum.LabelID.length)] // Use plain parent label ID for junction table query
  );
  for (const row of nestedLabelResults) {
    const redactedValue = await redactLabelValue(
      orgId,
      row.value,
      requesterUserId
    );
    if (redactedValue !== null) {
      nestedLabels.push(redactedValue);
    }
  }

  const labelFE: LabelFE = {
    ...label,
    resources: associatedResources,
    labels: nestedLabels,
    permission_previews: permissionPreviews,
  };

  // Apply redaction rules
  const redacted = { ...labelFE };
  if (!isOwner) {
    // Redact private_note unless user has edit permissions
    if (!permissionPreviews.includes(SystemPermissionType.EDIT)) {
      redacted.private_note = undefined;
    }
    // Redact resources list for non-owners
    redacted.resources = [];
  }

  // Labels (nested) are already filtered/redacted during their fetch.
  // external_id and external_payload are handled by the main redaction if necessary.
  redacted.external_id = label.external_id;
  redacted.external_payload = label.external_payload;

  return redacted;
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

    label = {
      id: rawLabel.id as LabelID,
      value: rawLabel.value as LabelValue,
      public_note: rawLabel.public_note,
      private_note: rawLabel.private_note,
      color: rawLabel.color,
      created_by: rawLabel.created_by_user_id as UserID,
      created_at: rawLabel.created_at,
      last_updated_at: rawLabel.last_updated_at,
      resources: [], // Will be populated by castLabelToLabelFE
      labels: [], // Will be populated by castLabelToLabelFE
      external_id: rawLabel.external_id,
      external_payload: rawLabel.external_payload,
    } as Label;

    // Permissions check: If not owner, explicitly check view permissions.
    if (!isOwner) {
      const tablePermissions = await checkSystemPermissions(
        // SystemResourceID for a table: TABLE_TABLE_NAME
        `TABLE_${SystemTableValueEnum.LABELS}`,
        requesterApiKey.user_id,
        org_id
      );

      const labelRecordId = `${IDPrefixEnum.LabelID}${label.id.substring(IDPrefixEnum.LabelID.length)}`;

      const resourcePermissions = await checkSystemPermissions(
        // SystemResourceID for a record: RecordID_UUID
        labelRecordId,
        requesterApiKey.user_id,
        org_id
      );

      const labelPrefixPermissions = await checkSystemResourcePermissionsLabels(
        // The resource here is implicitly the Labels table, but with a label value filter.
        // Rust's `check_system_resource_permissions_labels` takes SystemResourceID
        // and checks metadata type `Labels` with `LabelStringValuePrefix`.
        // A common pattern is to check against the general table (LABELS) for label-specific rules.
        `TABLE_${SystemTableValueEnum.LABELS}`,
        requesterApiKey.user_id,
        label.value,
        org_id
      );

      if (
        !tablePermissions.includes(SystemPermissionType.VIEW) &&
        !resourcePermissions.includes(SystemPermissionType.VIEW) &&
        !labelPrefixPermissions.includes(SystemPermissionType.VIEW)
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
      // FIX: The cursor is `created_at` timestamp in Rust, which is a number. Validate as a number.
      if (isNaN(Number(requestBody.cursor))) {
        validationErrors.push("cursor must be a valid number string");
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

    // Check table-level permission for the prefix (using checkSystemResourcePermissionsLabels for prefix-specific permissions)
    const tablePermissionsForPrefix =
      await checkSystemResourcePermissionsLabels(
        `TABLE_${SystemTableValueEnum.LABELS}`, // Check against the Labels table
        requesterApiKey.user_id,
        prefixFilter, // Use the prefix filter here
        org_id
      );
    const hasTablePermission = tablePermissionsForPrefix.includes(
      SystemPermissionType.VIEW
    );

    request.log.debug(`has_table_permission: ${hasTablePermission}`);

    // If user doesn't have table-level permissions and is not owner, return early with empty list.
    // Rust only returns labels if `has_table_permission` is true.
    if (!hasTablePermission && !isOwner) {
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
        // Rust uses >= or <= based on direction for cursor.
        cursorCondition = ` ${orderByField} ${direction === SortDirection.DESC ? "<=" : ">="} ?`; // Use <= for DESC, >= for ASC
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

    let labels: LabelFE[] = [];
    let nextCursor: string | null = null;

    if (rawLabels.length > pageSize) {
      const extraItem = rawLabels.pop(); // Remove the extra item
      if (extraItem) {
        nextCursor = String(extraItem.created_at); // Cursor for next page
      }
    }

    // Process labels, applying resource-level permissions and redaction
    for (const rawLabel of rawLabels) {
      const label: Label = {
        id: rawLabel.id as LabelID,
        value: rawLabel.value as LabelValue,
        public_note: rawLabel.public_note,
        private_note: rawLabel.private_note,
        color: rawLabel.color,
        created_by: rawLabel.created_by_user_id as UserID,
        created_at: rawLabel.created_at,
        last_updated_at: rawLabel.last_updated_at,
        resources: [], // Will be populated by castLabelToLabelFE
        labels: [], // Will be populated by castLabelToLabelFE
        external_id: rawLabel.external_id,
        external_payload: rawLabel.external_payload,
      };

      // Apply redaction rules using castLabelToLabelFE. This function now handles permission checks.
      const labelFE = await castLabelToLabelFE(
        label,
        requesterApiKey.user_id,
        org_id
      );

      // Only add to the list if the user has VIEW permission (checked inside castLabelToLabelFE or explicitly here)
      // The `LabelFE` returned by `castLabelToLabelFE` is already redacted for the `requesterUserId`.
      // The `redactLabelValue` in `permissions/system.ts` also performs a view check.
      // If the label itself is not viewable, its value would be redacted to null,
      // and it shouldn't be included in the list.
      // However, for the main list, we generally show all visible labels, even if some internal fields are redacted.
      // The primary check `hasTablePermission` covers overall visibility.
      // Individual label visibility is handled by `redactLabelValue` within `castLabelToLabelFE` for nested labels.
      labels.push(labelFE);
    }

    // Determine total count
    const countQuery = `SELECT COUNT(*) AS count FROM labels ${
      conditions.length > 0 ? "WHERE " + conditions.join(" AND ") : ""
    }`;
    const countParams = params.filter((_, idx) => idx < conditions.length); // Only use filter params, not cursor params for total count
    const totalResult = await db.queryDrive(org_id, countQuery, countParams);
    const totalCount = totalResult.length > 0 ? totalResult[0].count : 0;

    return reply.status(200).send(
      createApiResponse<IPaginatedResponse<LabelFE>>({
        items: labels, // labels is already LabelFE[]
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
      const isValid = validateExternalId(createReq.external_id);
      if (!isValid) validationErrors.push("Invalid external_id");
    }
    // Validate external_payload
    if (createReq.external_payload !== undefined) {
      const isValid = validateExternalPayload(createReq.external_payload);
      if (!isValid) validationErrors.push("Invalid external_payload");
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
        `TABLE_${SystemTableValueEnum.LABELS}`, // SystemResourceID for Labels table
        requesterApiKey.user_id,
        org_id
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

    // Validate provided ID if any
    if (createReq.id) {
      if (!createReq.id.startsWith(IDPrefixEnum.LabelID)) {
        validationErrors.push(
          `Label ID must start with '${IDPrefixEnum.LabelID}'.`
        );
      } else {
        // Check if the provided ID is already claimed
        const alreadyClaimed = await isUUIDClaimed(org_id, createReq.id);
        if (alreadyClaimed) {
          validationErrors.push(
            `Provided Label ID '${createReq.id}' is already claimed.`
          );
        }
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

    const current_time = Date.now(); // Milliseconds
    const labelId = (createReq.id ||
      `${IDPrefixEnum.LabelID}${uuidv4()}`) as LabelID;

    // Build the new label object
    const newLabel: Label = {
      id: labelId,
      value: validatedLabelValue as LabelValue,
      public_note: createReq.public_note || undefined, // Use null for optional string fields if empty
      private_note: createReq.private_note || undefined, // Use null for optional string fields if empty
      color: validatedColor,
      created_by: requesterApiKey.user_id,
      created_at: current_time,
      last_updated_at: current_time,
      resources: [], // New labels start with no resources
      labels: [], // New labels start with no nested labels
      external_id: createReq.external_id || undefined, // Use null for optional string fields if empty
      external_payload: createReq.external_payload || undefined, // Use null for optional string fields if empty
    };

    // Store the label in the database within a transaction
    await dbHelpers.transaction("drive", org_id, (database) => {
      // NOTE: The `resources` and `labels` columns are not in the provided SQL schema for the `labels` table.
      // Removed them from the INSERT statement to prevent SQL errors. They are derived properties in TS.
      database
        .prepare(
          `INSERT INTO labels (id, value, public_note, private_note, color, created_by_user_id, created_at, last_updated_at, external_id, external_payload)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
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
          newLabel.external_id,
          newLabel.external_payload
        );
      request.log.debug(`Created label ${newLabel.id}`);
    });

    // Update external ID mapping as per Rust's `update_external_id_mapping`
    if (newLabel.external_id) {
      await updateExternalIDMapping(
        org_id,
        undefined, // No old external ID for creation
        newLabel.external_id,
        newLabel.id
      );
    }

    // Mark the generated/provided LabelID as claimed in the `uuid_claimed` table.
    const successfullyClaimed = await claimUUID(org_id, newLabel.id);
    if (!successfullyClaimed) {
      // This case should ideally not be reached if validation (isUUIDClaimed) is perfect,
      // but provides a safeguard against very unlikely race conditions.
      throw new Error(
        `Failed to claim UUID for new label '${newLabel.id}'. It might have been claimed concurrently.`
      );
    }

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
    const isValidId = validateIdString(updateReq.id);
    if (!isValidId) validationErrors.push("Invalid label ID");

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
      "SELECT id, value, public_note, private_note, color, created_by_user_id, created_at, last_updated_at, external_id, external_payload FROM labels WHERE id = ?",
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
        `TABLE_${SystemTableValueEnum.LABELS}`, // SystemResourceID for Labels table
        requesterApiKey.user_id,
        org_id
      );

      const labelRecordId = `${IDPrefixEnum.LabelID}${labelId.substring(IDPrefixEnum.LabelID.length)}`;

      const resourcePermissions = await checkSystemPermissions(
        labelRecordId,
        requesterApiKey.user_id,
        org_id
      );

      const labelPrefixPermissions = await checkSystemResourcePermissionsLabels(
        `TABLE_${SystemTableValueEnum.LABELS}`,
        requesterApiKey.user_id,
        existingLabel.value,
        org_id
      );

      if (
        !tablePermissions.includes(SystemPermissionType.EDIT) &&
        !resourcePermissions.includes(SystemPermissionType.EDIT) &&
        !labelPrefixPermissions.includes(SystemPermissionType.EDIT)
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
    const oldLabelValue = existingLabel.value; // Store old value to propagate changes

    const updates: string[] = [];
    const params: any[] = [];

    // Now, add validation and updates for description and color
    if (updateReq.description !== undefined) {
      const validation = validateDescription(
        updateReq.description,
        "description"
      );
      if (!validation.valid) validationErrors.push(validation.error!);
      else {
        updates.push("public_note = ?"); // Assuming description maps to public_note
        params.push(updateReq.description);
        existingLabel.public_note = updateReq.description; // Update in memory for `castLabelToLabelFE`
      }
    }
    if (updateReq.color !== undefined) {
      const colorValidation = validateColor(updateReq.color);
      if (!colorValidation.valid) validationErrors.push(colorValidation.error!);
      else {
        const validatedColor = colorValidation.validatedColor;
        if (validatedColor) {
          updates.push("color = ?");
          params.push(validatedColor);
          existingLabel.color = validatedColor; // Update in memory for `castLabelToLabelFE`
        }
      }
    }

    let newLabelValue: LabelValue | undefined;
    if (updateReq.value !== undefined) {
      // Re-check value after initialization
      const validatedValueResult = validateLabelValue(updateReq.value);
      if (!validatedValueResult.valid || !validatedValueResult.validatedValue) {
        validationErrors.push(
          validatedValueResult.error || "Invalid label value"
        );
      } else {
        newLabelValue = validatedValueResult.validatedValue;
        if (newLabelValue !== oldLabelValue) {
          updates.push("value = ?");
          params.push(newLabelValue);
          existingLabel.value = newLabelValue; // Update in memory for `castLabelToLabelFE`
        }
      }
    }

    if (updateReq.private_note !== undefined) {
      const validation = validateDescription(
        updateReq.private_note,
        "private_note"
      );
      if (!validation.valid) validationErrors.push(validation.error!);
      else {
        updates.push("private_note = ?");
        params.push(updateReq.private_note);
        existingLabel.private_note = updateReq.private_note;
      }
    }

    if (updateReq.external_id !== undefined) {
      updates.push("external_id = ?");
      params.push(updateReq.external_id);
      existingLabel.external_id = updateReq.external_id;
      // Handle external ID mapping change
      await updateExternalIDMapping(
        org_id, // Pass the driveId (org_id)
        oldExternalId, // The external_id before the update
        updateReq.external_id, // The new external_id from the request body
        labelId // The internal LabelID
      );
    }
    if (updateReq.external_payload !== undefined) {
      updates.push("external_payload = ?");
      params.push(updateReq.external_payload);
      existingLabel.external_payload = updateReq.external_payload;
    }

    if (validationErrors.length > 0) {
      // Check validation errors again after all validations
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validationErrors.join("; "),
        })
      );
    }

    if (updates.length === 0 && newLabelValue === oldLabelValue) {
      // No actual changes
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
    await dbHelpers.transaction("drive", org_id, async (database) => {
      // If label value is changing, update all associated resources
      if (newLabelValue && newLabelValue !== oldLabelValue) {
        // Fetch all resources currently associated with this label
        const junctionTables = [
          {
            name: "api_key_labels",
            resourceIdColumn: "api_key_id",
            targetTable: "api_keys",
            prefix: IDPrefixEnum.ApiKey,
          },
          {
            name: "contact_labels",
            resourceIdColumn: "user_id",
            targetTable: "contacts",
            prefix: IDPrefixEnum.User,
          },
          {
            name: "file_labels",
            resourceIdColumn: "file_id",
            targetTable: "files",
            prefix: IDPrefixEnum.File,
          },
          {
            name: "folder_labels",
            resourceIdColumn: "folder_id",
            targetTable: "folders",
            prefix: IDPrefixEnum.Folder,
          },
          {
            name: "disk_labels",
            resourceIdColumn: "disk_id",
            targetTable: "disks",
            prefix: IDPrefixEnum.Disk,
          },
          {
            name: "drive_labels",
            resourceIdColumn: "drive_id",
            targetTable: "drives",
            prefix: IDPrefixEnum.Drive,
          },
          {
            name: "group_labels",
            resourceIdColumn: "group_id",
            targetTable: "groups",
            prefix: IDPrefixEnum.Group,
          },
          {
            name: "group_invite_labels",
            resourceIdColumn: "invite_id",
            targetTable: "group_invites",
            prefix: IDPrefixEnum.GroupInvite,
          },
          {
            name: "permission_directory_labels",
            resourceIdColumn: "permission_id",
            targetTable: "permissions_directory",
            prefix: IDPrefixEnum.DirectoryPermission,
          },
          {
            name: "permission_system_labels",
            resourceIdColumn: "permission_id",
            targetTable: "permissions_system",
            prefix: IDPrefixEnum.SystemPermission,
          },
          {
            name: "webhook_labels",
            resourceIdColumn: "webhook_id",
            targetTable: "webhooks",
            prefix: IDPrefixEnum.Webhook,
          },
          {
            name: "label_labels",
            resourceIdColumn: "parent_label_id",
            targetTable: "labels",
            prefix: IDPrefixEnum.LabelID,
          }, // Labels can label other labels
        ];

        // Fetch resources that currently have this label.
        const affectedResources: {
          resourceId: string;
          tableName: string;
          timestampCol: string;
        }[] = [];
        for (const {
          name: junctionTableName,
          resourceIdColumn,
          targetTable,
          prefix,
        } of junctionTables) {
          const rows = database
            .prepare(
              `SELECT ${resourceIdColumn} FROM ${junctionTableName} WHERE label_id = ?`
            )
            .all(labelId);
          for (const row of rows as { [resourceIdColumn: string]: string }[]) {
            // Need to know the actual resource ID (prefixed) and its table name to update.
            const fullResourceId = `${prefix}${row[resourceIdColumn]}`;
            let timestampColumn = "last_updated_at"; // Default
            if (
              [
                "permissions_directory",
                "permissions_system",
                "group_invites",
                "groups",
              ].includes(targetTable)
            ) {
              timestampColumn = "last_modified_at";
            }
            affectedResources.push({
              resourceId: fullResourceId,
              tableName: targetTable,
              timestampCol: timestampColumn,
            });
          }
        }

        for (const {
          resourceId,
          tableName,
          timestampCol,
        } of affectedResources) {
          const currentLabelsRaw = database
            .prepare(`SELECT labels FROM ${tableName} WHERE id = ?`)
            .get(resourceId) as { labels: string };
          let currentLabels: string[] = [];
          try {
            currentLabels = currentLabelsRaw?.labels
              ? JSON.parse(currentLabelsRaw.labels)
              : [];
          } catch (e) {
            console.warn(
              `Failed to parse labels for resource ${resourceId}: ${e}`
            );
          }

          // Remove old label value
          const updatedLabels = currentLabels.filter(
            (label) => label !== oldLabelValue
          );
          // Add new label value if not already present
          if (!updatedLabels.includes(newLabelValue)) {
            updatedLabels.push(newLabelValue);
          }

          // Update the resource's labels and timestamp
          database
            .prepare(
              `UPDATE ${tableName} SET labels = ?, ${timestampCol} = ? WHERE id = ?`
            )
            .run(JSON.stringify(updatedLabels), Date.now(), resourceId);
        }
      }

      // Update the label itself in the labels table
      database
        .prepare(`UPDATE labels SET ${updates.join(", ")} WHERE id = ?`)
        .run(...params, labelId);
      request.log.debug(`Updated label ${labelId}`);
    });

    // Re-fetch the updated label to ensure all derived fields are current
    const updatedLabelResult = await db.queryDrive(
      org_id,
      "SELECT id, value, public_note, private_note, color, created_by_user_id, created_at, last_updated_at, external_id, external_payload FROM labels WHERE id = ?",
      [labelId]
    );

    if (!updatedLabelResult || updatedLabelResult.length === 0) {
      // This should ideally not happen if the update succeeded
      throw new Error("Failed to retrieve updated label.");
    }
    const updatedRawLabel = updatedLabelResult[0];

    // Reconstruct Label object from DB result for FE casting
    const finalUpdatedLabel: Label = {
      id: updatedRawLabel.id as LabelID,
      value: updatedRawLabel.value as LabelValue,
      public_note: updatedRawLabel.public_note,
      private_note: updatedRawLabel.private_note,
      color: updatedRawLabel.color,
      created_by: updatedRawLabel.created_by_user_id as UserID,
      created_at: updatedRawLabel.created_at,
      last_updated_at: updatedRawLabel.last_updated_at,
      resources: [], // Will be populated by castLabelToLabelFE
      labels: [], // Will be populated by castLabelToLabelFE
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
    const isValid = validateIdString(body.id);
    if (!isValid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Invalid label ID",
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
        `TABLE_${SystemTableValueEnum.LABELS}`, // SystemResourceID for Labels table
        requesterApiKey.user_id,
        org_id
      );

      const labelRecordId = `${IDPrefixEnum.LabelID}${labelId.substring(IDPrefixEnum.LabelID.length)}`;

      const resourcePermissions = await checkSystemPermissions(
        labelRecordId,
        requesterApiKey.user_id,
        org_id
      );

      const labelPrefixPermissions = await checkSystemResourcePermissionsLabels(
        `TABLE_${SystemTableValueEnum.LABELS}`,
        requesterApiKey.user_id,
        labelValue,
        org_id
      );

      if (
        !tablePermissions.includes(SystemPermissionType.DELETE) &&
        !resourcePermissions.includes(SystemPermissionType.DELETE) &&
        !labelPrefixPermissions.includes(SystemPermissionType.DELETE)
      ) {
        return reply
          .status(403)
          .send(
            createApiResponse(undefined, { code: 403, message: "Forbidden" })
          );
      }
    }

    // Perform delete operation within a transaction
    await dbHelpers.transaction("drive", org_id, async (database) => {
      // 1. Remove label associations from all junction tables
      // Need to find all resources associated with this label first
      const junctionTablesMeta = [
        {
          name: "api_key_labels",
          resourceIdColumn: "api_key_id",
          targetTable: "api_keys",
          prefix: IDPrefixEnum.ApiKey,
        },
        {
          name: "contact_labels",
          resourceIdColumn: "user_id",
          targetTable: "contacts",
          prefix: IDPrefixEnum.User,
        },
        {
          name: "file_labels",
          resourceIdColumn: "file_id",
          targetTable: "files",
          prefix: IDPrefixEnum.File,
        },
        {
          name: "folder_labels",
          resourceIdColumn: "folder_id",
          targetTable: "folders",
          prefix: IDPrefixEnum.Folder,
        },
        {
          name: "disk_labels",
          resourceIdColumn: "disk_id",
          targetTable: "disks",
          prefix: IDPrefixEnum.Disk,
        },
        {
          name: "drive_labels",
          resourceIdColumn: "drive_id",
          targetTable: "drives",
          prefix: IDPrefixEnum.Drive,
        },
        {
          name: "group_labels",
          resourceIdColumn: "group_id",
          targetTable: "groups",
          prefix: IDPrefixEnum.Group,
        },
        {
          name: "group_invite_labels",
          resourceIdColumn: "invite_id",
          targetTable: "group_invites",
          prefix: IDPrefixEnum.GroupInvite,
        },
        {
          name: "permission_directory_labels",
          resourceIdColumn: "permission_id",
          targetTable: "permissions_directory",
          prefix: IDPrefixEnum.DirectoryPermission,
        },
        {
          name: "permission_system_labels",
          resourceIdColumn: "permission_id",
          targetTable: "permissions_system",
          prefix: IDPrefixEnum.SystemPermission,
        },
        {
          name: "webhook_labels",
          resourceIdColumn: "webhook_id",
          targetTable: "webhooks",
          prefix: IDPrefixEnum.Webhook,
        },
        {
          name: "label_labels",
          resourceIdColumn: "parent_label_id",
          targetTable: "labels",
          prefix: IDPrefixEnum.LabelID,
        }, // Labels can label other labels
      ];

      const affectedResources: {
        resourceId: string;
        tableName: string;
        timestampCol: string;
      }[] = [];
      const plainLabelId = labelId.substring(IDPrefixEnum.LabelID.length);

      // Collect all resources whose `labels` array needs to be updated
      for (const {
        name: junctionTableName,
        resourceIdColumn,
        targetTable,
        prefix,
      } of junctionTablesMeta) {
        let rows;
        if (junctionTableName === "label_labels") {
          // For label_labels, this label can be a parent or a child
          rows = database
            .prepare(
              `SELECT parent_label_id AS resource_id_val FROM ${junctionTableName} WHERE child_label_id = ? UNION SELECT child_label_id AS resource_id_val FROM ${junctionTableName} WHERE parent_label_id = ?`
            )
            .all(plainLabelId, plainLabelId);
        } else {
          rows = database
            .prepare(
              `SELECT ${resourceIdColumn} AS resource_id_val FROM ${junctionTableName} WHERE label_id = ?`
            )
            .all(plainLabelId);
        }

        for (const row of rows as { resource_id_val: string }[]) {
          const fullResourceId = `${prefix}${row.resource_id_val}`;
          let timestampColumn = "last_updated_at"; // Default
          if (
            [
              "permissions_directory",
              "permissions_system",
              "group_invites",
              "groups",
            ].includes(targetTable)
          ) {
            timestampColumn = "last_modified_at";
          }
          // Ensure we don't add the label itself as an "affected resource" for its own label field update.
          // The label itself is being deleted, so its 'labels' field (nested labels) will disappear with it.
          if (fullResourceId !== labelId) {
            affectedResources.push({
              resourceId: fullResourceId,
              tableName: targetTable,
              timestampCol: timestampColumn,
            });
          }
        }
      }

      // Update the `labels` column on affected resources by removing the deleted label's value
      for (const { resourceId, tableName, timestampCol } of affectedResources) {
        const currentLabelsRaw = database
          .prepare(`SELECT labels FROM ${tableName} WHERE id = ?`)
          .get(resourceId) as { labels: string };
        let currentLabels: string[] = [];
        try {
          currentLabels = currentLabelsRaw?.labels
            ? JSON.parse(currentLabelsRaw.labels)
            : [];
        } catch (e) {
          console.warn(
            `Failed to parse labels for resource ${resourceId} during delete cleanup: ${e}`
          );
        }

        const updatedLabels = currentLabels.filter(
          (label) => label !== labelValue
        );

        database
          .prepare(
            `UPDATE ${tableName} SET labels = ?, ${timestampCol} = ? WHERE id = ?`
          )
          .run(JSON.stringify(updatedLabels), Date.now(), resourceId);
      }

      // Delete from all junction tables where this label is referenced
      for (const { name: junctionTableName } of junctionTablesMeta) {
        if (junctionTableName === "label_labels") {
          // Handle both parent_label_id and child_label_id for label_labels
          database
            .prepare(
              `DELETE FROM ${junctionTableName} WHERE parent_label_id = ? OR child_label_id = ?`
            )
            .run(plainLabelId, plainLabelId);
        } else {
          database
            .prepare(`DELETE FROM ${junctionTableName} WHERE label_id = ?`)
            .run(plainLabelId);
        }
        request.log.debug(
          `Deleted from ${junctionTableName} for label ${labelId}`
        );
      }

      // 2. Delete the label itself from the `labels` table
      database.prepare("DELETE FROM labels WHERE id = ?").run(labelId);
      request.log.debug(`Deleted label ${labelId} from labels table.`);
    });

    if (oldExternalId) {
      await updateExternalIDMapping(
        org_id, // Pass the driveId (org_id)
        oldExternalId, // The external_id to remove
        undefined, // No new external ID (signal for removal)
        oldInternalId // The internal LabelID
      );
    }

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
    let validation = validateIdString(body.label_id);
    if (!validation) validationErrors.push("Invalid label ID");
    validation = validateIdString(body.resource_id);
    if (!validation) validationErrors.push("Invalid resource ID");

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
      "SELECT id, value, public_note, private_note, color, created_by_user_id, created_at, last_updated_at, external_id, external_payload FROM labels WHERE id = ?",
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
    const plainLabelId = labelId.substring(IDPrefixEnum.LabelID.length); // Plain ID for junction tables

    // Parse resource ID and determine its type
    const parsedResource = parseLabelResourceID(resourceIdString);
    if (
      !parsedResource.valid ||
      !parsedResource.resourceId ||
      !parsedResource.resourceTypePrefix ||
      !parsedResource.resourceTableName
    ) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: parsedResource.error!,
        })
      );
    }
    const actualResourceId = parsedResource.resourceId; // This is the full ID string, e.g., "FileID_xyz"
    const resourceTypePrefix = parsedResource.resourceTypePrefix; // e.g., "FileID"
    const resourceTableName = parsedResource.resourceTableName; // e.g., "files"
    const actualPlainResourceId = actualResourceId.substring(
      resourceTypePrefix.length + 1
    );

    // Check permissions if not owner
    if (!isOwner) {
      // Check table-level permissions for 'Labels'
      const tablePermissions = await checkSystemPermissions(
        `TABLE_${SystemTableValueEnum.LABELS}`, // SystemResourceID for Labels table
        requesterApiKey.user_id,
        org_id
      );

      // Construct the SystemResourceID for the specific resource being labeled/unlabeled
      let resourceSystemId: SystemResourceID;
      // Handle special case where resourceTypePrefix is "LabelID" (labels labeling other labels)
      if (resourceTypePrefix === IDPrefixEnum.LabelID.slice(0, -1)) {
        resourceSystemId = actualResourceId; // LabelID_uuid format
      } else {
        resourceSystemId = actualResourceId; // For other record types, the `resourceIdString` (e.g., "FileID_xyz") is already the correct SystemResourceID.
      }

      const resourceBeingLabeledPermissions = await checkSystemPermissions(
        resourceSystemId, // The full resource ID string (e.g., "FileID_xyz")
        requesterApiKey.user_id,
        org_id
      );

      // Check label-specific permissions on the target resource (if applicable, e.g. permission to label a file)
      // The Rust code's `check_system_resource_permissions_labels` is used here.
      const labelSpecificResourcePermissions =
        await checkSystemResourcePermissionsLabels(
          resourceSystemId, // Resource being labeled
          requesterApiKey.user_id,
          labelValue, // The label being added/removed
          org_id
        );

      if (
        !(
          tablePermissions.includes(SystemPermissionType.EDIT) ||
          resourceBeingLabeledPermissions.includes(SystemPermissionType.EDIT) ||
          labelSpecificResourcePermissions.includes(SystemPermissionType.EDIT)
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
      let junctionTableResourceIdColumn = "";

      // Determine junction table name and resource ID column
      switch (resourceTypePrefix) {
        case IDPrefixEnum.ApiKey.slice(0, -1):
          junctionTableName = "api_key_labels";
          junctionTableResourceIdColumn = "api_key_id";
          break;
        case IDPrefixEnum.User.slice(0, -1):
          junctionTableName = "contact_labels";
          junctionTableResourceIdColumn = "user_id";
          break;
        case IDPrefixEnum.File.slice(0, -1):
          junctionTableName = "file_labels";
          junctionTableResourceIdColumn = "file_id";
          break;
        case IDPrefixEnum.Folder.slice(0, -1):
          junctionTableName = "folder_labels";
          junctionTableResourceIdColumn = "folder_id";
          break;
        case IDPrefixEnum.Disk.slice(0, -1):
          junctionTableName = "disk_labels";
          junctionTableResourceIdColumn = "disk_id";
          break;
        case IDPrefixEnum.Drive.slice(0, -1):
          junctionTableName = "drive_labels";
          junctionTableResourceIdColumn = "drive_id";
          break;
        case IDPrefixEnum.DirectoryPermission.slice(0, -1):
          junctionTableName = "permission_directory_labels";
          junctionTableResourceIdColumn = "permission_id";
          break;
        case IDPrefixEnum.SystemPermission.slice(0, -1):
          junctionTableName = "permission_system_labels";
          junctionTableResourceIdColumn = "permission_id";
          break;
        case IDPrefixEnum.GroupInvite.slice(0, -1):
          junctionTableName = "group_invite_labels";
          junctionTableResourceIdColumn = "invite_id";
          break;
        case IDPrefixEnum.Group.slice(0, -1):
          junctionTableName = "group_labels";
          junctionTableResourceIdColumn = "group_id";
          break;
        case IDPrefixEnum.Webhook.slice(0, -1):
          junctionTableName = "webhook_labels";
          junctionTableResourceIdColumn = "webhook_id";
          break;
        case IDPrefixEnum.LabelID.slice(0, -1): // Label can be labeled itself
          junctionTableName = "label_labels";
          junctionTableResourceIdColumn = "parent_label_id"; // When a label is being labeled, it's the parent
          break;
        default:
          throw new Error(
            `Unsupported resource type for labeling: ${resourceTypePrefix}`
          );
      }

      // 1. Update the resource's `labels` column (stored as JSON string in SQLite)
      const existingResourceRow = database
        .prepare(`SELECT labels FROM ${resourceTableName} WHERE id = ?`)
        .get(actualResourceId) as { labels: string | null }; // Type assertion

      let currentResourceLabels: string[] = [];
      if (existingResourceRow && existingResourceRow.labels) {
        try {
          currentResourceLabels = JSON.parse(existingResourceRow.labels);
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
            junctionTableParams = [actualPlainResourceId, plainLabelId]; // parent_label_id = resource_id, child_label_id = label_id
          } else {
            junctionTableActionSql = `INSERT INTO ${junctionTableName} (${junctionTableResourceIdColumn}, label_id) VALUES (?, ?)`;
            junctionTableParams = [actualPlainResourceId, plainLabelId];
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
            junctionTableParams = [actualPlainResourceId, plainLabelId];
          } else {
            junctionTableActionSql = `DELETE FROM ${junctionTableName} WHERE ${junctionTableResourceIdColumn} = ? AND label_id = ?`;
            junctionTableParams = [actualPlainResourceId, plainLabelId];
          }
          database.prepare(junctionTableActionSql).run(...junctionTableParams);
        }
      }

      // Update the resource's labels column
      let updateResourceSql = `UPDATE ${resourceTableName} SET labels = ?`;
      const updateResourceParams: any[] = [
        JSON.stringify(updatedResourceLabels),
      ];

      // Add `last_updated_at` or `last_modified_at` for relevant tables
      const hasTimestamp = [
        "files",
        "folders",
        "api_keys",
        "contacts",
        "disks",
        "drives",
        "groups",
        "group_invites",
        "webhooks",
        "labels", // When a label is also a resource and its labels are updated
      ].includes(resourceTableName);

      if (hasTimestamp) {
        let timestampColumnName = "last_updated_at";
        if (
          [
            "permissions_directory",
            "permissions_system",
            "group_invites",
            "groups",
          ].includes(resourceTableName)
        ) {
          timestampColumnName = "last_modified_at";
        }
        updateResourceSql += `, ${timestampColumnName} = ?`;
        updateResourceParams.push(Date.now());
      }
      updateResourceSql += ` WHERE id = ?`;
      updateResourceParams.push(actualResourceId);

      database.prepare(updateResourceSql).run(...updateResourceParams);
      request.log.debug(
        `${addOperation ? "Added" : "Removed"} label ${labelValue} to resource ${actualResourceId}`
      );

      // Handle label deletion if its resources become empty, mirroring Rust behavior
      if (!addOperation) {
        // Only if removing a label
        // Check how many resources still point to this label
        const junctionTablesMeta = [
          // FIX: Defined junctionTablesMeta here
          {
            name: "api_key_labels",
            resourceIdColumn: "api_key_id",
            targetTable: "api_keys",
            prefix: IDPrefixEnum.ApiKey,
          },
          {
            name: "contact_labels",
            resourceIdColumn: "user_id",
            targetTable: "contacts",
            prefix: IDPrefixEnum.User,
          },
          {
            name: "file_labels",
            resourceIdColumn: "file_id",
            targetTable: "files",
            prefix: IDPrefixEnum.File,
          },
          {
            name: "folder_labels",
            resourceIdColumn: "folder_id",
            targetTable: "folders",
            prefix: IDPrefixEnum.Folder,
          },
          {
            name: "disk_labels",
            resourceIdColumn: "disk_id",
            targetTable: "disks",
            prefix: IDPrefixEnum.Disk,
          },
          {
            name: "drive_labels",
            resourceIdColumn: "drive_id",
            targetTable: "drives",
            prefix: IDPrefixEnum.Drive,
          },
          {
            name: "group_labels",
            resourceIdColumn: "group_id",
            targetTable: "groups",
            prefix: IDPrefixEnum.Group,
          },
          {
            name: "group_invite_labels",
            resourceIdColumn: "invite_id",
            targetTable: "group_invites",
            prefix: IDPrefixEnum.GroupInvite,
          },
          {
            name: "permission_directory_labels",
            resourceIdColumn: "permission_id",
            targetTable: "permissions_directory",
            prefix: IDPrefixEnum.DirectoryPermission,
          },
          {
            name: "permission_system_labels",
            resourceIdColumn: "permission_id",
            targetTable: "permissions_system",
            prefix: IDPrefixEnum.SystemPermission,
          },
          {
            name: "webhook_labels",
            resourceIdColumn: "webhook_id",
            targetTable: "webhooks",
            prefix: IDPrefixEnum.Webhook,
          },
          {
            name: "label_labels",
            resourceIdColumn: "parent_label_id",
            targetTable: "labels",
            prefix: IDPrefixEnum.LabelID,
          }, // Labels can label other labels
        ];
        let totalResourcesForLabel = 0;
        for (const {
          name: junctionTableName,
          resourceIdColumn,
        } of junctionTablesMeta) {
          // Now junctionTablesMeta is defined
          if (junctionTableName === "label_labels") {
            // For label_labels, count where this label is a child
            const count = database
              .prepare(
                `SELECT COUNT(*) as count FROM ${junctionTableName} WHERE child_label_id = ?`
              )
              .get(plainLabelId) as { count: number };
            totalResourcesForLabel += count.count;
          } else {
            const count = database
              .prepare(
                `SELECT COUNT(*) as count FROM ${junctionTableName} WHERE label_id = ?`
              )
              .get(plainLabelId) as { count: number };
            totalResourcesForLabel += count.count;
          }
        }

        if (totalResourcesForLabel === 0) {
          request.log.debug(
            `Label '${labelValue}' (ID: ${labelId}) no longer has any resources. Deleting label.`
          );
          database.prepare("DELETE FROM labels WHERE id = ?").run(labelId);
          // Also delete associated nested label relationships where this label is a child if it's being deleted
          database
            .prepare(`DELETE FROM label_labels WHERE child_label_id = ?`)
            .run(plainLabelId);
          await updateExternalIDMapping(
            org_id,
            rawLabel.external_id,
            undefined, // null signals removal
            labelId
          ); // Clean up external ID mapping
        }
      }
    });

    const actionMessage = addOperation ? "added" : "removed";

    // Call the actual `getActiveLabelWebhooks` and `fireLabelWebhook` from the service.
    const webhooksToFire = await getActiveLabelWebhooks(
      org_id, // Pass orgId
      labelId,
      addOperation
        ? WebhookEventLabel.LABEL_ADDED
        : WebhookEventLabel.LABEL_REMOVED
    );

    if (webhooksToFire.length > 0) {
      const notes = `Label ${labelId} ${actionMessage} resource ${actualResourceId}`;
      const webhookPayloadData: LabelWebhookData = {
        label_id: labelId,
        resource_id: actualResourceId,
        label_value: labelValue,
        add: addOperation,
      };
      await fireLabelWebhook(
        addOperation
          ? WebhookEventLabel.LABEL_ADDED
          : WebhookEventLabel.LABEL_REMOVED,
        webhooksToFire,
        undefined, // No `before` snapshot for this specific event in Rust
        webhookPayloadData, // `after` snapshot as per Rust
        notes
      );
    }

    // Re-fetch the label to return its updated state (if it still exists)
    let finalLabel: Label | undefined;
    const finalLabelQuery = await db.queryDrive(
      org_id,
      "SELECT id, value, public_note, private_note, color, created_by_user_id, created_at, last_updated_at, external_id, external_payload FROM labels WHERE id = ?",
      [labelId]
    );

    if (finalLabelQuery.length > 0) {
      const rawFinalLabel = finalLabelQuery[0];
      finalLabel = {
        id: rawFinalLabel.id as LabelID,
        value: rawFinalLabel.value as LabelValue,
        public_note: rawFinalLabel.public_note,
        private_note: rawFinalLabel.private_note,
        color: rawFinalLabel.color,
        created_by: rawFinalLabel.created_by_user_id as UserID,
        created_at: rawFinalLabel.created_at,
        last_updated_at: rawFinalLabel.last_updated_at,
        resources: [], // Will be populated by castLabelToLabelFE
        labels: [], // Will be populated by castLabelToLabelFE
        external_id: rawFinalLabel.external_id,
        external_payload: rawFinalLabel.external_payload,
      };
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
