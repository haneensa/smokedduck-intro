import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  stats: defineTable({
    sessionID: v.string(),
    q: v.string(),
  }),
  bugs: defineTable({
    sessionID: v.string(),
    query_log: v.string(),
    csv: v.string(),
    comment: v.string(),
    email: v.string()
  })
});
