import { mutation } from "./_generated/server";
import { v } from "convex/values";

export const sendstats = mutation({
  args: {
    sessionID: v.string(),
    q: v.string()
  },
  handler: async (ctx, args) => {
    const { sessionID, q  } = args;
    await ctx.db.insert("stats", { sessionID, q } );
  },
});

export const sendbug = mutation({
  args: {
    sessionID: v.string(),
    query_log: v.string(),
    csv: v.string(),
    comment: v.string(),
    email: v.string()
  },
  handler: async (ctx, args) => {
    const { sessionID, query_log, csv, comment, email } = args;
    await ctx.db.insert("bugs", { sessionID, query_log, csv, comment, email });
  },
});
