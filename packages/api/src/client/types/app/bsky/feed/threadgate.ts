/**
 * GENERATED CODE - DO NOT MODIFY
 */
import { ValidationResult, BlobRef } from '@atproto/lexicon'
import { isObj, hasProp } from '../../../../util'
import { lexicons } from '../../../../lexicons'
import { CID } from 'multiformats/cid'

export interface Record {
  /** Reference (AT-URI) to the post record. */
  post: string
  /** List of rules defining who can reply to this post. If value is an empty array, no one can reply. If value is undefined, anyone can reply. */
  allow?: (
    | MentionRule
    | FollowerRule
    | FollowingRule
    | ListRule
    | { $type: string; [k: string]: unknown }
  )[]
  createdAt: string
  /** List of hidden reply URIs. */
  hiddenReplies?: string[]
  [k: string]: unknown
}

export function isRecord(v: unknown): v is Record {
  return (
    isObj(v) &&
    hasProp(v, '$type') &&
    (v.$type === 'app.bsky.feed.threadgate#main' ||
      v.$type === 'app.bsky.feed.threadgate')
  )
}

export function validateRecord(v: unknown): ValidationResult {
  return lexicons.validate('app.bsky.feed.threadgate#main', v)
}

/** Allow replies from actors mentioned in your post. */
export interface MentionRule {
  [k: string]: unknown
}

export function isMentionRule(v: unknown): v is MentionRule {
  return (
    isObj(v) &&
    hasProp(v, '$type') &&
    v.$type === 'app.bsky.feed.threadgate#mentionRule'
  )
}

export function validateMentionRule(v: unknown): ValidationResult {
  return lexicons.validate('app.bsky.feed.threadgate#mentionRule', v)
}

/** Allow replies from actors who follow you. */
export interface FollowerRule {
  [k: string]: unknown
}

export function isFollowerRule(v: unknown): v is FollowerRule {
  return (
    isObj(v) &&
    hasProp(v, '$type') &&
    v.$type === 'app.bsky.feed.threadgate#followerRule'
  )
}

export function validateFollowerRule(v: unknown): ValidationResult {
  return lexicons.validate('app.bsky.feed.threadgate#followerRule', v)
}

/** Allow replies from actors you follow. */
export interface FollowingRule {
  [k: string]: unknown
}

export function isFollowingRule(v: unknown): v is FollowingRule {
  return (
    isObj(v) &&
    hasProp(v, '$type') &&
    v.$type === 'app.bsky.feed.threadgate#followingRule'
  )
}

export function validateFollowingRule(v: unknown): ValidationResult {
  return lexicons.validate('app.bsky.feed.threadgate#followingRule', v)
}

/** Allow replies from actors on a list. */
export interface ListRule {
  list: string
  [k: string]: unknown
}

export function isListRule(v: unknown): v is ListRule {
  return (
    isObj(v) &&
    hasProp(v, '$type') &&
    v.$type === 'app.bsky.feed.threadgate#listRule'
  )
}

export function validateListRule(v: unknown): ValidationResult {
  return lexicons.validate('app.bsky.feed.threadgate#listRule', v)
}
