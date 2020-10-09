# frozen_string_literal: true

require 'mysql2'
require File.expand_path(File.dirname(__FILE__) + "/base.rb")

class ImportScripts::Bbpress < ImportScripts::Base

  BB_PRESS_HOST            ||= ENV['BBPRESS_HOST'] || "localhost"
  BB_PRESS_DB              ||= ENV['BBPRESS_DB'] || "bbpress"
  BATCH_SIZE               ||= 1000
  BB_PRESS_PW              ||= ENV['BBPRESS_PW'] || ""
  BB_PRESS_USER            ||= ENV['BBPRESS_USER'] || "root"
  BB_PRESS_PREFIX          ||= ENV['BBPRESS_PREFIX'] || "wp_"
  BB_PRESS_ATTACHMENTS_DIR ||= ENV['BBPRESS_ATTACHMENTS_DIR'] || "/path/to/attachments"

  def initialize
    super

    @he = HTMLEntities.new

    @client = Mysql2::Client.new(
      host: BB_PRESS_HOST,
      username: BB_PRESS_USER,
      database: BB_PRESS_DB,
      password: BB_PRESS_PW,
    )
  end

  def execute
    import_users
    import_categories
    import_topics_and_posts
    associate_categories_to_topics
    import_comments_and_staged_users
  end

  def import_users
    puts "", "importing users..."

    last_user_id = -1
    total_users = bbpress_query(<<-SQL
      SELECT COUNT(DISTINCT(u.id)) AS cnt
      FROM #{BB_PRESS_PREFIX}users u
      LEFT JOIN #{BB_PRESS_PREFIX}posts p ON p.post_author = u.id
      WHERE p.post_type IN ('dwqa-question', 'dwqa-answer')
      AND p.post_status = 'publish'
        AND user_email LIKE '%@%'
    SQL
    ).first["cnt"]

    batches(BATCH_SIZE) do |offset|
      users = bbpress_query(<<-SQL
        SELECT u.id, user_nicename, display_name, user_email, user_registered, user_url, user_pass
          FROM #{BB_PRESS_PREFIX}users u
          LEFT JOIN #{BB_PRESS_PREFIX}posts p ON p.post_author = u.id
         WHERE user_email LIKE '%@%'
         AND p.post_type IN ('dwqa-question', 'dwqa-answer')
         AND p.post_status = 'publish'
           AND u.id > #{last_user_id}
      GROUP BY u.id
      ORDER BY u.id
         LIMIT #{BATCH_SIZE}
      SQL
      ).to_a

      break if users.empty?

      last_user_id = users[-1]["id"]
      user_ids = users.map { |u| u["id"].to_i }

      next if all_records_exist?(:users, user_ids)

      user_ids_sql = user_ids.join(",")

      create_users(users, total: total_users, offset: offset) do |u|
        {
          id: u["id"].to_i,
          username: u["user_nicename"],
          password: u["user_pass"],
          email: u["user_email"].downcase,
          name: u["display_name"].presence || u['user_nicename'],
          created_at: u["user_registered"],
          website: u["user_url"],
        }
      end
    end
  end

  def import_categories
    puts "", "importing categories..."

    categories = bbpress_query(<<-SQL
    SELECT DISTINCT t.term_id as id, 
                    t.term_taxonomy_id, 
                    terms.name AS category_name, 
                    terms.slug AS slug, 
                    t.description FROM wp_term_taxonomy AS t 
                    LEFT JOIN wp_term_relationships AS r 
                    ON r.term_taxonomy_id = t.term_taxonomy_id 
                    LEFT JOIN wp_terms AS terms 
                    ON terms.term_id = t.term_id 
                    WHERE taxonomy = 'dwqa-question_category';
    SQL
    )

    create_categories(categories) do |c|
      category = { id: c['id'], name: c['category_name'] }
      category
    end
  end

  def import_topics_and_posts
    puts "", "importing topics and posts..."

    last_post_id = -1
    total_posts = bbpress_query(<<-SQL
      SELECT COUNT(*) count
        FROM #{BB_PRESS_PREFIX}posts
        WHERE post_status = 'publish'
        AND post_type IN ('dwqa-question', 'dwqa-answer')
    SQL
    ).first["count"]

    batches(BATCH_SIZE) do |offset|
      posts = bbpress_query(<<-SQL
        SELECT id,
               post_author,
               post_date,
               post_content,
               post_title,
               post_type,
               post_parent
          FROM #{BB_PRESS_PREFIX}posts
          WHERE post_status = 'publish'
          AND post_type IN ('dwqa-question', 'dwqa-answer')
          AND id > #{last_post_id}
      ORDER BY id
         LIMIT #{BATCH_SIZE}
      SQL
      ).to_a

      break if posts.empty?

      last_post_id = posts[-1]["id"].to_i
      post_ids = posts.map { |p| p["id"].to_i }

      next if all_records_exist?(:posts, post_ids)

      post_ids_sql = post_ids.join(",")

      create_posts(posts, total: total_posts, offset: offset) do |p|
        skip = false

        user_id = user_id_from_imported_user_id(p["post_author"]) ||
                  find_user_by_import_id(p["post_author"]).try(:id) ||
                  # user_id_from_imported_user_id(anon_names[p['id']]) ||
                  # find_user_by_import_id(anon_names[p['id']]).try(:id) ||
                  -1

        post = {
          id: p["id"],
          user_id: user_id,
          raw: p["post_content"],
          created_at: p["post_date"],
          # like_count: posts_likes[p["id"]],
        }

        if post[:raw].present?
          post[:raw].gsub!(/\<pre\>\<code(=[a-z]*)?\>(.*?)\<\/code\>\<\/pre\>/im) { "```\n#{@he.decode($2)}\n```" }
        end

        if p["post_type"] == "dwqa-question"
          post[:category] = category_id_from_imported_category_id(p["post_parent"])
          post[:title] = CGI.unescapeHTML(p["post_title"])
        else
          if parent = topic_lookup_from_imported_post_id(p["post_parent"])
            post[:topic_id] = parent[:topic_id]
            post[:reply_to_post_number] = parent[:post_number] if parent[:post_number] > 1
          else
            puts "Skipping #{p["id"]}: #{p["post_content"][0..40]}"
            skip = true
          end
        end

        skip ? nil : post
      end
    end
  end


  def import_comments_and_staged_users
    puts "", "importing comments and anonymous commenters ;) ..."
    comment_id_offset = 50000
    last_comment_id = -1
    total_comments = bbpress_query(<<-SQL
      SELECT COUNT(*) count
        FROM #{BB_PRESS_PREFIX}comments wpc
        LEFT JOIN wp_posts p 
          ON wpc.comment_post_ID = p.ID 
          WHERE p.post_type = 'dwqa-answer' 
          AND p.post_status = 'publish'
          AND wpc.comment_approved = 1
    SQL
    ).first["count"]

    batches(BATCH_SIZE) do |offset|
      comments = bbpress_query(<<-SQL
        SELECT comment_ID+#{comment_id_offset} as id,
               comment_post_ID,
               comment_author,
               comment_author_email,
               comment_content,
               comment_date,
               user_id
          FROM #{BB_PRESS_PREFIX}comments wpc
          LEFT JOIN wp_posts p 
          ON wpc.comment_post_ID = p.ID 
          WHERE p.post_type = 'dwqa-answer' 
          AND p.post_status = 'publish'
          AND wpc.comment_approved = 1
          AND wpc.comment_ID+#{comment_id_offset} > #{last_comment_id}
      ORDER BY id
         LIMIT #{BATCH_SIZE}
      SQL
      ).to_a

      break if comments.empty?

      last_comment_id = comments[-1]["id"].to_i
      comment_ids = comments.map { |p| p["id"].to_i}

      next if all_records_exist?(:posts, comment_ids)

      post_ids_sql = comment_ids.join(",")

      create_posts(comments, total: total_comments, offset: offset) do |p|
        skip = false

        user_id = user_id_from_imported_user_id(p["user_id"]) ||
                  find_user_by_import_id(p["user_id"]).try(:id) ||
                  -1

        if user_id == -1
          params = {
            username: p['comment_author'],
            email: p['comment_author_email'],
            staged: true
          }
          user = create_user(params, p['comment_author'])
          user_id = user[:id]
          p "created a new staged user #{user_id} from comment"
        end
        post = {
          id: p["id"],
          user_id: user_id,
          raw: p["comment_content"],
          created_at: p["comment_date"],
          # like_count: posts_likes[p["id"]],
        }

        if post[:raw].present?
          post[:raw].gsub!(/\<pre\>\<code(=[a-z]*)?\>(.*?)\<\/code\>\<\/pre\>/im) { "```\n#{@he.decode($2)}\n```" }
        end

        if parent = post_id_from_imported_post_id(p["comment_post_ID"])
           post_obj = Post.find(parent)
          post[:topic_id] = post_obj.topic_id
          post[:reply_to_post_number] = post_obj[:post_number] if post_obj[:post_number] > 1
        else
          puts "Skipping #{p["id"]}: #{p["comment_content"][0..40]}"
          skip = true
        end
        

        skip ? nil : post
      end
    end
  end

  def associate_categories_to_topics
    p 'asociating category to topics'
    topic_count = bbpress_query(<<-SQL
      SELECT COUNT(*) as cnt
      FROM wp_posts AS p 
      LEFT JOIN wp_term_relationships AS tr 
      ON p.id = tr.object_id 
      LEFT JOIN wp_term_taxonomy AS tt 
      ON tr.term_taxonomy_id = tt.term_taxonomy_id 
      LEFT JOIN wp_terms AS wpt 
      ON tt.term_id = wpt.term_id 
      WHERE post_status = 'publish' 
      AND post_type IN ('dwqa-question') 
      AND taxonomy = "dwqa-question_category" 
      ORDER BY id
    SQL
    ).first['cnt']
    last_topic_id = -1

    batches(BATCH_SIZE) do |offset|
      category_assoc = bbpress_query(<<-SQL
      SELECT p.id, 
      tt.term_id 
      FROM wp_posts AS p 
      LEFT JOIN wp_term_relationships AS tr 
      ON p.id = tr.object_id 
      LEFT JOIN wp_term_taxonomy AS tt 
      ON tr.term_taxonomy_id = tt.term_taxonomy_id 
      LEFT JOIN wp_terms AS wpt 
      ON tt.term_id = wpt.term_id 
      WHERE post_status = 'publish' 
      AND post_type IN ('dwqa-question') 
      AND taxonomy = "dwqa-question_category"
      AND id > #{last_topic_id}
      ORDER BY id
      LIMIT #{BATCH_SIZE}
      
    SQL
    ).to_a
    break if category_assoc.empty?
    last_topic_id = category_assoc[-1]["id"].to_i
    
    category_assoc.each do |row|
      post_id = post_id_from_imported_post_id(row['id'])
      topic = Post.find(post_id).topic
      category_id = category_id_from_imported_category_id(row['term_id'])
      next if !topic || !category_id
      topic.category_id = category_id
      topic.save
      p "category #{category_id} updated for topic #{topic.id}"
    end
    end
  end

  def bbpress_query(sql)
    @client.query(sql, cache_rows: false)
  end

end

ImportScripts::Bbpress.new.perform
