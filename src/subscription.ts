import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  private metrics = {
    totalEvents: 0,
    eventsPerSecond: 0,
    startTime: Date.now(),
    lastMetricsUpdate: Date.now(),
    matchingPosts: 0
  }

  private updateMetrics() {
    const now = Date.now()
    const timeSinceLastUpdate = (now - this.metrics.lastMetricsUpdate) / 1000
    this.metrics.eventsPerSecond = Math.round(this.metrics.totalEvents /
      ((now - this.metrics.startTime) / 1000))

    this.metrics.lastMetricsUpdate = now
  }

  private incrementEventCount() {
    this.metrics.totalEvents++
  }

  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return

    const ops = await getOpsByType(evt)

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        // Filter for ocaml-related posts from last 48 hours
        const postTime = new Date(create.record.createdAt).getTime()
        const timeCutoff = Date.now() - (48 * 60 * 60 * 1000)
        const matches = postTime > timeCutoff &&
          (create.record.text.toLowerCase().includes('ocaml')
           || create.record.text.toLowerCase().includes('🐫'))
        if (matches) this.metrics.matchingPosts++
        return matches
      })
      .map((create) => {
        console.log(create.record.text);
        console.log(create.author);

        return {
          uri: create.uri,
          cid: create.cid,
          indexedAt: new Date().toISOString(),
        }
      })

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }

  async run(subscriptionReconnectDelay: number) {
    try {
      for await (const evt of this.sub) {
        this.incrementEventCount()
        this.updateMetrics()
        this.handleEvent(evt).catch((err) => {
          console.error('repo subscription could not handle message', err)
        })
        // update stored cursor every 20 events or so
        if (isCommit(evt) && evt.seq % 20 === 0) {
          await this.updateCursor(evt.seq)
        }
        if (isCommit(evt) && evt.seq % 100 === 0) {
          // Log metrics
          console.log(`Firehose Progress:
            Total Events: ${this.metrics.totalEvents}
            Events/second: ${this.metrics.eventsPerSecond}
            Matching Posts: ${this.metrics.matchingPosts}
            Running time: ${Math.round((Date.now() - this.metrics.startTime) / 1000)}s
          `)
        }
      }
    } catch (err) {
      console.error('repo subscription errored', err)
      setTimeout(
        () => this.run(subscriptionReconnectDelay),
        subscriptionReconnectDelay,
      )
    }
  }
}
