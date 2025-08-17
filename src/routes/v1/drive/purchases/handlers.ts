import { FastifyReply, FastifyRequest } from "fastify";
import { createApiResponse, getDriveOwnerId, OrgIdParams } from "../../types";
import {
  DriveID,
  GenerateID,
  IDPrefixEnum,
  IPaginatedResponse,
  IRequestCreatePurchase,
  IRequestDeletePurchase,
  IRequestGetPurchase,
  IRequestListPurchases,
  IRequestUpdatePurchase,
  IResponseDeletePurchase,
  Purchase,
  PurchaseFE,
  PurchaseStatus,
  SortDirection,
  SystemPermissionType,
  SystemResourceID,
  SystemTableValueEnum,
} from "@officexapp/types";
import { authenticateRequest } from "../../../../services/auth";
import { db, dbHelpers } from "../../../../services/database";
import {
  validateDescription,
  validateIdString,
  validateShortString,
  validateUrl,
} from "../../../../services/validation";
import { claimUUID, isUUIDClaimed } from "../../../../services/external";
import {
  redactLabelValue,
  checkSystemPermissions,
} from "../../../../services/permissions/system";
import { GetPurchaseParams } from ".";
import { trackEvent } from "../../../../services/analytics";

export async function getPurchaseHandler(
  request: FastifyRequest<{ Params: GetPurchaseParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id, purchase_id: purchaseId } = request.params;

    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    const purchases = await db.queryDrive(
      org_id,
      "SELECT * FROM purchases WHERE id = ?",
      [purchaseId]
    );

    if (!purchases || purchases.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Purchase not found",
        })
      );
    }

    const purchase = purchases[0] as Purchase;

    const permissionPreviews = isOwner
      ? [
          SystemPermissionType.CREATE,
          SystemPermissionType.EDIT,
          SystemPermissionType.DELETE,
          SystemPermissionType.VIEW,
          SystemPermissionType.INVITE,
        ]
      : await Promise.resolve().then(async () => {
          const permissions = await checkSystemPermissions({
            resourceTable: `TABLE_${SystemTableValueEnum.PURCHASES}`,
            resourceId: `${purchase.id}` as SystemResourceID,
            granteeId: requesterApiKey.user_id,
            orgId: org_id,
          });
          return Array.from(new Set([...permissions]));
        });

    const purchaseFE: PurchaseFE = {
      ...purchase,
      permission_previews: permissionPreviews,
      related_resources: [], // Ensure related_resources is an empty array as it's no longer used
    };

    // Redaction logic based on Rust's PurchaseFE::redacted
    const isVendorOfPurchase = requesterApiKey.user_id === purchase.vendor_id;
    const hasTableViewPermission = purchaseFE.permission_previews.includes(
      SystemPermissionType.VIEW
    );

    if (!isVendorOfPurchase && !hasTableViewPermission) {
      purchaseFE.notes = "";
    }
    if (!isVendorOfPurchase && !hasTableViewPermission) {
      purchaseFE.vendor_notes = "";
      purchaseFE.tracer = undefined;
    }

    const purchaseLabelsRaw = await db.queryDrive(
      org_id,
      `SELECT T2.value FROM purchase_labels AS T1 JOIN labels AS T2 ON T1.label_id = T2.id WHERE T1.purchase_id = ?`,
      [purchase.id]
    );
    purchaseFE.labels = (
      await Promise.all(
        purchaseLabelsRaw.map((row: any) =>
          redactLabelValue(org_id, row.value, requesterApiKey.user_id)
        )
      )
    ).filter((label): label is string => label !== null);

    // Removed purchase_related_resources query

    return reply.status(200).send(createApiResponse(purchaseFE));
  } catch (error) {
    request.log.error("Error in getPurchaseHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function listPurchasesHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestListPurchases }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const requestBody = request.body;

    // 1. Authenticate the request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }
    const requesterUserId = requesterApiKey.user_id;

    // 2. Validate request body
    const pageSize = requestBody.page_size || 50;
    if (pageSize === 0 || pageSize > 1000) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: page_size - Page size must be between 1 and 1000",
        })
      );
    }
    if (requestBody.filters && requestBody.filters.length > 256) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: filters - Filters must be 256 characters or less",
        })
      );
    }
    if (requestBody.cursor && requestBody.cursor.length > 256) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: cursor - Cursor must be 256 characters or less",
        })
      );
    }

    // 3. Construct the dynamic SQL query
    const direction = requestBody.direction || SortDirection.DESC;
    const filters = requestBody.filters || "";

    let query = `SELECT * FROM purchases`;
    const queryParams: any[] = [];
    const whereClauses: string[] = [];

    if (filters) {
      // Assuming a simple filter on the purchase's name or some other field
      // NOTE: This part needs to be implemented based on the actual schema
      whereClauses.push(`(some_field LIKE ?)`);
      queryParams.push(`%${filters}%`);
    }

    if (whereClauses.length > 0) {
      query += ` WHERE ${whereClauses.join(" AND ")}`;
    }

    query += ` ORDER BY created_at ${direction}`;

    let offset = 0;
    if (requestBody.cursor) {
      offset = parseInt(requestBody.cursor, 10);
      if (isNaN(offset)) {
        return reply.status(400).send(
          createApiResponse(undefined, {
            code: 400,
            message: "Invalid cursor format",
          })
        );
      }
    }

    query += ` LIMIT ? OFFSET ?`;
    queryParams.push(pageSize, offset);

    // 4. Execute the query
    const rawPurchases = await db.queryDrive(org_id, query, queryParams);

    // 5. Process and filter purchases based on permissions
    const processedPurchases: PurchaseFE[] = [];

    for (const purchase of rawPurchases) {
      const purchaseRecordResourceId: string = `${purchase.id}`;

      const permissions = await checkSystemPermissions({
        resourceTable: `TABLE_${SystemTableValueEnum.PURCHASES}`,
        resourceId: purchaseRecordResourceId,
        granteeId: requesterUserId,
        orgId: org_id,
      });

      if (permissions.includes(SystemPermissionType.VIEW)) {
        const purchaseFE: PurchaseFE = {
          ...(purchase as Purchase),
          labels: [],
          related_resources: [],
          permission_previews: permissions,
        };

        // Redaction logic based on ownership and permissions
        const isVendorOfPurchase = requesterUserId === purchase.vendor_id;
        const hasEditPermission = permissions.includes(
          SystemPermissionType.EDIT
        );

        if (!isVendorOfPurchase && !hasEditPermission) {
          purchaseFE.notes = "";
          purchaseFE.vendor_notes = "";
          purchaseFE.tracer = undefined;
        }

        // Fetch and redact labels
        const listPurchaseLabelsRaw = await db.queryDrive(
          org_id,
          `SELECT T2.value FROM purchase_labels AS T1 JOIN labels AS T2 ON T1.label_id = T2.id WHERE T1.purchase_id = ?`,
          [purchase.id]
        );
        purchaseFE.labels = (
          await Promise.all(
            listPurchaseLabelsRaw.map((row: any) =>
              redactLabelValue(org_id, row.value, requesterUserId)
            )
          )
        ).filter((label): label is string => label !== null);

        processedPurchases.push(purchaseFE);
      }
    }

    // 6. Handle pagination and total count
    const nextCursor =
      processedPurchases.length < pageSize
        ? null
        : (offset + pageSize).toString();

    const totalCountToReturn = processedPurchases.length;

    return reply.status(200).send(
      createApiResponse<IPaginatedResponse<PurchaseFE>>({
        items: processedPurchases,
        page_size: processedPurchases.length,
        total: totalCountToReturn,
        direction: direction,
        cursor: nextCursor || undefined,
      })
    );
  } catch (error) {
    request.log.error("Error in listPurchasesHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function createPurchaseHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestCreatePurchase;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const body = request.body;

    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    const validation = await validateCreatePurchaseRequest(body, org_id);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    const userPermissions = await checkSystemPermissions({
      resourceTable: `TABLE_${SystemTableValueEnum.PURCHASES}`,
      granteeId: requesterApiKey.user_id,
      orgId: org_id,
    });

    const hasCreatePermission =
      isOwner || userPermissions.includes(SystemPermissionType.CREATE);

    if (!hasCreatePermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const purchaseId = body.id || GenerateID.PurchaseID();
    const now = Date.now();

    const newPurchase: Purchase = await dbHelpers.transaction(
      "drive",
      org_id,
      (database) => {
        claimUUID(database, purchaseId);
        const insertPurchaseStmt = database.prepare(
          `INSERT INTO purchases (id, template_id, vendor_name, vendor_id, status, description, about_url, billing_url, support_url, delivery_url, verification_url, installation_url, title, subtitle, pricing, vendor_notes, notes, created_at, updated_at, last_updated_at, tracer, external_id, external_payload)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
        );
        insertPurchaseStmt.run(
          purchaseId,
          body.template_id || null,
          body.vendor_name || "Unknown Vendor",
          body.vendor_id,
          body.status || PurchaseStatus.REQUESTED, // Default status if not provided
          body.description || null,
          body.about_url || "",
          body.billing_url || null,
          body.support_url || null,
          body.delivery_url || null,
          body.verification_url || null,
          body.auth_installation_url || null,
          body.title,
          body.subtitle || null,
          body.pricing || null,
          body.vendor_notes || null,
          body.notes || null,
          now,
          now,
          now,
          body.tracer || null,
          body.external_id || null,
          body.external_payload || null
        );

        if (body.labels && body.labels.length > 0) {
          const insertLabelStmt = database.prepare(
            `INSERT INTO purchase_labels (purchase_id, label_id) VALUES (?, ?)`
          );
          for (const labelId of body.labels) {
            insertLabelStmt.run(purchaseId, labelId);
          }
        }

        const createdPurchase: Purchase = {
          id: purchaseId,
          template_id: body.template_id,
          vendor_name: body.vendor_name || "",
          vendor_id: body.vendor_id || "",
          status: body.status || PurchaseStatus.REQUESTED,
          description: body.description || "",
          about_url: body.about_url || "",
          billing_url: body.billing_url || "",
          support_url: body.support_url || "",
          delivery_url: body.delivery_url || "",
          verification_url: body.verification_url || "",
          auth_installation_url: body.auth_installation_url || "",
          title: body.title || "",
          subtitle: body.subtitle || "",
          pricing: body.pricing || "",
          next_delivery_date: body.next_delivery_date || -1,
          vendor_notes: body.vendor_notes || "",
          notes: body.notes || "",
          created_at: now,
          updated_at: now,
          last_updated_at: now,
          labels: body.labels || [],
          related_resources: [], // Ensure related_resources is empty
          tracer: body.tracer,
          external_id: body.external_id,
          external_payload: body.external_payload,
        };
        return createdPurchase;
      }
    );

    const permissionPreviews = await checkSystemPermissions({
      resourceTable: `TABLE_${SystemTableValueEnum.PURCHASES}`,
      granteeId: requesterApiKey.user_id,
      orgId: org_id,
    });

    const purchaseFE: PurchaseFE = {
      ...newPurchase,
      permission_previews: permissionPreviews,
    };

    const isVendorOfPurchase =
      requesterApiKey.user_id === newPurchase.vendor_id;
    const hasTableViewPermission = purchaseFE.permission_previews.includes(
      SystemPermissionType.VIEW
    );

    if (!isVendorOfPurchase && !hasTableViewPermission) {
      purchaseFE.notes = "";
    }
    if (!isVendorOfPurchase && !hasTableViewPermission) {
      purchaseFE.vendor_notes = "";
      purchaseFE.tracer = undefined;
    }

    const createPurchaseLabelsRaw = await db.queryDrive(
      org_id,
      `SELECT T2.value FROM purchase_labels AS T1 JOIN labels AS T2 ON T1.label_id = T2.id WHERE T1.purchase_id = ?`,
      [newPurchase.id]
    );
    purchaseFE.labels = (
      await Promise.all(
        createPurchaseLabelsRaw.map((row: any) =>
          redactLabelValue(org_id, row.value, requesterApiKey.user_id)
        )
      )
    ).filter((label): label is string => label !== null);

    purchaseFE.related_resources = [];

    trackEvent("create_purchase", {
      purchase_id: newPurchase.id,
      drive_id: org_id,
    });

    return reply.status(200).send(createApiResponse(purchaseFE));
  } catch (error) {
    request.log.error("Error in createPurchaseHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function updatePurchaseHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestUpdatePurchase;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const body = request.body;

    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    const validation = await validateUpdatePurchaseRequest(body, org_id);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    const purchaseId = body.id;

    const existingPurchases = await db.queryDrive(
      org_id,
      "SELECT * FROM purchases WHERE id = ?",
      [purchaseId]
    );

    if (!existingPurchases || existingPurchases.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Purchase not found",
        })
      );
    }
    const existingPurchase = existingPurchases[0] as Purchase;

    const hasEditPermission = await checkSystemPermissions({
      resourceTable: `TABLE_${SystemTableValueEnum.PURCHASES}`,
      resourceId: `${purchaseId}` as SystemResourceID,
      granteeId: requesterApiKey.user_id,
      orgId: org_id,
    });

    if (!isOwner && !hasEditPermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const updates: string[] = [];
    const values: any[] = [];
    const now = Date.now();

    if (body.title !== undefined) {
      updates.push("title = ?");
      values.push(body.title);
    }
    if (body.subtitle !== undefined) {
      updates.push("subtitle = ?");
      values.push(body.subtitle);
    }
    if (body.description !== undefined) {
      updates.push("description = ?");
      values.push(body.description);
    }
    if (body.about_url !== undefined) {
      updates.push("about_url = ?");
      values.push(body.about_url);
    }
    if (body.delivery_url !== undefined) {
      updates.push("delivery_url = ?");
      values.push(body.delivery_url);
    }
    if (body.verification_url !== undefined) {
      updates.push("verification_url = ?");
      values.push(body.verification_url);
    }
    if (body.status !== undefined) {
      updates.push("status = ?");
      values.push(body.status);
    }
    if (body.billing_url !== undefined) {
      updates.push("billing_url = ?");
      values.push(body.billing_url);
    }
    if (body.support_url !== undefined) {
      updates.push("support_url = ?");
      values.push(body.support_url);
    }
    if (body.delivery_url !== undefined) {
      updates.push("delivery_url = ?");
      values.push(body.delivery_url);
    }
    if (body.verification_url !== undefined) {
      updates.push("verification_url = ?");
      values.push(body.verification_url);
    }
    if (body.subtitle !== undefined) {
      updates.push("subtitle = ?");
      values.push(body.subtitle);
    }
    if (body.pricing !== undefined) {
      updates.push("pricing = ?");
      values.push(body.pricing);
    }
    if (body.vendor_notes !== undefined) {
      updates.push("vendor_notes = ?");
      values.push(body.vendor_notes);
    }
    if (body.tracer !== undefined) {
      updates.push("tracer = ?");
      values.push(body.tracer);
    }
    if (body.external_id !== undefined) {
      updates.push("external_id = ?");
      values.push(body.external_id);
    }
    if (body.external_payload !== undefined) {
      updates.push("external_payload = ?");
      values.push(body.external_payload);
    }

    updates.push("updated_at = ?");
    values.push(now);
    updates.push("last_updated_at = ?");
    values.push(now); // Assuming last_updated_at is also updated

    if (
      updates.length === 2 &&
      updates.includes("updated_at = ?") &&
      updates.includes("last_updated_at = ?")
    ) {
      // Only contains timestamp updates
      // This means no user-provided fields are being updated.
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "No fields to update",
        })
      );
    }

    await dbHelpers.transaction("drive", org_id, (database) => {
      const stmt = database.prepare(
        `UPDATE purchases SET ${updates.join(", ")} WHERE id = ?`
      );
      stmt.run(...values, purchaseId);

      // Handle labels
      if (body.labels !== undefined) {
        database
          .prepare(`DELETE FROM purchase_labels WHERE purchase_id = ?`)
          .run(purchaseId);
        if (body.labels.length > 0) {
          const insertLabelStmt = database.prepare(
            `INSERT INTO purchase_labels (purchase_id, label_id) VALUES (?, ?)`
          );
          for (const labelId of body.labels) {
            insertLabelStmt.run(purchaseId, labelId);
          }
        }
      }
    });

    const updatedPurchases = await db.queryDrive(
      org_id,
      "SELECT * FROM purchases WHERE id = ?",
      [purchaseId]
    );
    const updatedPurchase = updatedPurchases[0] as Purchase;

    const permissionPreviews = await checkSystemPermissions({
      resourceTable: `TABLE_${SystemTableValueEnum.PURCHASES}`,
      resourceId: `${purchaseId}` as SystemResourceID,
      granteeId: requesterApiKey.user_id,
      orgId: org_id,
    });

    const purchaseFE: PurchaseFE = {
      ...updatedPurchase,
      permission_previews: permissionPreviews,
    };

    const isVendorOfPurchase =
      requesterApiKey.user_id === updatedPurchase.vendor_id;
    const hasTableViewPermission = purchaseFE.permission_previews.includes(
      SystemPermissionType.VIEW
    );

    if (!isVendorOfPurchase && !hasTableViewPermission) {
      purchaseFE.notes = "";
    }
    if (!isVendorOfPurchase && !hasTableViewPermission) {
      purchaseFE.vendor_notes = "";
      purchaseFE.tracer = undefined;
    }

    const updatePurchaseLabelsRaw = await db.queryDrive(
      org_id,
      `SELECT T2.value FROM purchase_labels AS T1 JOIN labels AS T2 ON T1.label_id = T2.id WHERE T1.purchase_id = ?`,
      [updatedPurchase.id]
    );
    purchaseFE.labels = (
      await Promise.all(
        updatePurchaseLabelsRaw.map((row: any) =>
          redactLabelValue(org_id, row.value, requesterApiKey.user_id)
        )
      )
    ).filter((label): label is string => label !== null);

    purchaseFE.related_resources = []; // Ensure related_resources is empty

    return reply.status(200).send(createApiResponse(purchaseFE));
  } catch (error) {
    request.log.error("Error in updatePurchaseHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function deletePurchaseHandler(
  request: FastifyRequest<{
    Params: OrgIdParams;
    Body: IRequestDeletePurchase;
  }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const body = request.body;

    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    const validation = validateDeletePurchaseRequest(body);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    const purchaseId = body.id;

    const hasDeletePermission = (
      await checkSystemPermissions({
        resourceTable: `TABLE_${SystemTableValueEnum.PURCHASES}`,
        resourceId: `${purchaseId}` as SystemResourceID,
        granteeId: requesterApiKey.user_id,
        orgId: org_id,
      })
    ).includes(SystemPermissionType.DELETE);

    if (!isOwner && !hasDeletePermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const purchasesToDelete = await db.queryDrive(
      org_id,
      "SELECT external_id FROM purchases WHERE id = ?",
      [purchaseId]
    );
    const externalIdToDelete =
      purchasesToDelete.length > 0 ? purchasesToDelete[0].external_id : null;

    await dbHelpers.transaction("drive", org_id, (database) => {
      database.prepare("DELETE FROM purchases WHERE id = ?").run(purchaseId);
      database
        .prepare("DELETE FROM purchase_labels WHERE purchase_id = ?")
        .run(purchaseId);
      // Removed purchase_related_resources deletion
    });

    const deletedData: IResponseDeletePurchase["ok"]["data"] = {
      id: purchaseId,
      deleted: true,
    };

    return reply.status(200).send(createApiResponse(deletedData));
  } catch (error) {
    request.log.error("Error in deletePurchaseHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

async function validateCreatePurchaseRequest(
  body: IRequestCreatePurchase,
  orgID: DriveID
): Promise<{
  valid: boolean;
  error?: string;
}> {
  if (body.id) {
    const is_claimed = await isUUIDClaimed(body.id, orgID);
    if (is_claimed) {
      return {
        valid: false,
        error: "UUID is already claimed",
      };
    }
  }

  if (body.id) {
    if (!body.id.startsWith(IDPrefixEnum.PurchaseID)) {
      return {
        valid: false,
        error: `Purchase ID must start with '${IDPrefixEnum.PurchaseID}'.`,
      };
    }
  }

  let validation: { valid: boolean; error?: string };

  if (body.vendor_name) {
    validation = validateShortString(body.vendor_name, "vendor_name");
    if (!validation.valid) return validation;
  }

  if (body.vendor_id) {
    validation = validateShortString(body.vendor_id, "vendor_id");
    if (!validation.valid) return validation;
  }

  if (body.vendor_notes) {
    validation = validateDescription(body.vendor_notes, "vendor_notes");
    if (!validation.valid) return validation;
  }

  if (body.vendor_id) {
    validation = { valid: validateIdString(body.vendor_id) };
    if (!validation.valid) return validation;
  }

  if (body.vendor_name) {
    validation = validateShortString(body.vendor_name, "vendor_name");
    if (!validation.valid) return validation;
  }

  if (body.description) {
    validation = validateDescription(body.description, "description");
    if (!validation.valid) return validation;
  }

  if (body.about_url) {
    const is_valid = validateUrl(body.about_url);
    if (!is_valid) return { valid: false, error: "about_url is required." };
  }

  if (body.title) {
    validation = validateShortString(body.title, "title");
    if (!validation.valid) return validation;
  }

  if (body.notes) {
    validation = validateDescription(body.notes, "notes");
    if (!validation.valid) return validation;
  }

  if (body.template_id) {
    validation = validateShortString(body.template_id, "template_id");
    if (!validation.valid) return validation;
  }

  if (body.vendor_id) {
    validation = validateShortString(body.vendor_id, "vendor_id");
    if (!validation.valid) return validation;
  }

  if (body.vendor_name) {
    validation = validateShortString(body.vendor_name, "vendor_name");
    if (!validation.valid) return validation;
  }

  if (body.vendor_notes) {
    validation = validateDescription(body.vendor_notes, "vendor_notes");
    if (!validation.valid) return validation;
  }

  if (body.support_url) {
    const is_valid = validateUrl(body.support_url);
    if (!is_valid) return { valid: false, error: "support_url is required." };
  }

  validation = validateShortString(body.title, "title");
  if (!validation.valid) return validation;

  if (body.notes) {
    validation = validateDescription(body.notes, "notes");
    if (!validation.valid) return validation;
  }

  if (body.about_url) {
    // about_url is mandatory but has validation.
    const is_valid = validateUrl(body.about_url);
    if (!is_valid) return { valid: false, error: "about_url is required." };
  }

  if (body.billing_url) {
    const is_valid = validateUrl(body.billing_url);
    if (!is_valid) return { valid: false, error: "billing_url is required." };
  }
  if (body.support_url) {
    const is_valid = validateUrl(body.support_url);
    if (!is_valid) return { valid: false, error: "support_url is required." };
  }
  if (body.delivery_url) {
    const is_valid = validateUrl(body.delivery_url);
    if (!is_valid) return { valid: false, error: "delivery_url is required." };
  }
  if (body.verification_url) {
    const is_valid = validateUrl(body.verification_url);
    if (!is_valid)
      return { valid: false, error: "verification_url is required." };
  }
  if (body.auth_installation_url) {
    const is_valid = validateUrl(body.auth_installation_url);
    if (!is_valid)
      return { valid: false, error: "auth_installation_url is required." };
  }

  if (body.subtitle) {
    const is_valid = validateShortString(body.subtitle, "subtitle");
    if (!is_valid) return { valid: false, error: "subtitle is required." };
  }
  if (body.pricing) {
    const is_valid = validateShortString(body.pricing, "pricing");
    if (!is_valid) return { valid: false, error: "pricing is required." };
  }
  if (body.next_delivery_date) {
    const is_valid = !isNaN(body.next_delivery_date);
    if (!is_valid)
      return { valid: false, error: "next_delivery_date must be a number." };
  }
  if (body.vendor_notes) {
    const is_valid = validateDescription(body.vendor_notes, "vendor_notes");
    if (!is_valid) return { valid: false, error: "vendor_notes is required." };
  }
  if (body.tracer) {
    const is_valid = validateShortString(body.tracer, "tracer");
    if (!is_valid) return { valid: false, error: "tracer is required." };
  }

  if (body.labels) {
    for (const label of body.labels) {
      const is_valid = validateShortString(label, "label");
      if (!is_valid) return { valid: false, error: "label is required." };
    }
  }

  if (body.external_id) {
    const is_valid = validateIdString(body.external_id);
    if (!is_valid) return { valid: false, error: "external_id is required." };
  }
  if (body.external_payload) {
    const is_valid = validateDescription(
      body.external_payload,
      "external_payload"
    );
    if (!validation.valid) return validation;
  }

  return { valid: true };
}

async function validateUpdatePurchaseRequest(
  body: IRequestUpdatePurchase,
  orgID: DriveID
): Promise<{
  valid: boolean;
  error?: string;
}> {
  let is_valid = validateIdString(body.id);
  if (!is_valid) return { valid: false, error: "id is required." };

  if (body.billing_url) {
    is_valid = validateUrl(body.billing_url);
  }
  if (body.support_url) {
    is_valid = validateUrl(body.support_url);
    if (!is_valid) return { valid: false, error: "support_url is required." };
  }
  if (body.delivery_url) {
    is_valid = validateUrl(body.delivery_url);
    if (!is_valid) return { valid: false, error: "delivery_url is required." };
  }
  if (body.verification_url) {
    is_valid = validateUrl(body.verification_url);
    if (!is_valid)
      return { valid: false, error: "verification_url is required." };
  }

  if (body.subtitle) {
    is_valid = validateShortString(body.subtitle, "subtitle").valid;
    if (!is_valid) return { valid: false, error: "subtitle is required." };
  }
  if (body.pricing) {
    is_valid = validateShortString(body.pricing, "pricing").valid;
    if (!is_valid) return { valid: false, error: "pricing is required." };
  }
  if (body.next_delivery_date) {
    is_valid = !isNaN(body.next_delivery_date);
    if (!is_valid)
      return { valid: false, error: "next_delivery_date must be a number." };
  }
  if (body.vendor_notes) {
    is_valid = validateDescription(body.vendor_notes, "vendor_notes").valid;
    if (!is_valid) return { valid: false, error: "vendor_notes is required." };
  }
  if (body.tracer) {
    is_valid = validateShortString(body.tracer, "tracer").valid;
    if (!is_valid) return { valid: false, error: "tracer is required." };
  }

  if (body.labels) {
    for (const label of body.labels) {
      is_valid = validateShortString(label, "label").valid;
      if (!is_valid) return { valid: false, error: "label is required." };
    }
  }
  if (body.external_id) {
    is_valid = validateIdString(body.external_id);
    if (!is_valid) return { valid: false, error: "external_id is required." };
  }
  if (body.external_payload) {
    is_valid = validateDescription(
      body.external_payload,
      "external_payload"
    ).valid;
    if (!is_valid)
      return { valid: false, error: "external_payload is required." };
  }

  return { valid: true };
}

function validateDeletePurchaseRequest(body: IRequestDeletePurchase): {
  valid: boolean;
  error?: string;
} {
  const is_valid = validateIdString(body.id);
  if (!is_valid) return { valid: false, error: "Invalid ID" };

  if (!body.id.startsWith(IDPrefixEnum.PurchaseID)) {
    return {
      valid: false,
      error: `Purchase ID must start with '${IDPrefixEnum.PurchaseID}'.`,
    };
  }
  return { valid: true };
}

function validateListPurchasesRequest(body: IRequestListPurchases): {
  valid: boolean;
  error?: string;
} {
  if (body.filters && body.filters.length > 256) {
    return {
      valid: false,
      error: "Filters must be 256 characters or less",
    };
  }
  if (
    body.page_size !== undefined &&
    (body.page_size === 0 || body.page_size > 1000)
  ) {
    return {
      valid: false,
      error: "Page size must be between 1 and 1000",
    };
  }
  if (body.cursor && body.cursor.length > 256) {
    return { valid: false, error: "Cursor must be 256 characters or less" };
  }
  return { valid: true };
}
