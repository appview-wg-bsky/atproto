/**
* GENERATED CODE - DO NOT MODIFY
*/
import { Headers, XRPCError } from '@atproto/xrpc'

export interface QueryParams {
  author: string;
  limit?: number;
  before?: string;
}

export interface CallOptions {
  headers?: Headers;
}

export type InputSchema = undefined

export interface OutputSchema {
  cursor?: string;
  feed: FeedItem[];
}
export interface FeedItem {
  uri: string;
  cid: string;
  author: Actor;
  trendedBy?: Actor;
  repostedBy?: Actor;
  record: {};
  embed?: RecordEmbed | ExternalEmbed | UnknownEmbed;
  replyCount: number;
  repostCount: number;
  upvoteCount: number;
  downvoteCount: number;
  indexedAt: string;
  myState?: {
    repost?: string,
    upvote?: string,
    downvote?: string,
  };
}
export interface Actor {
  did: string;
  handle: string;
  actorType: string;
  displayName?: string;
}
export interface RecordEmbed {
  type: 'record';
  author: Actor;
  record: {};
}
export interface ExternalEmbed {
  type: 'external';
  uri: string;
  title: string;
  description: string;
  imageUri: string;
}
export interface UnknownEmbed {
  type: string;
}

export interface Response {
  success: boolean;
  headers: Headers;
  data: OutputSchema;
}

export function toKnownErr(e: any) {
  if (e instanceof XRPCError) {
  }
  return e
}