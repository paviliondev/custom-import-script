# frozen_string_literal: true

require 'mysql2'
require File.expand_path(File.dirname(__FILE__) + "/base.rb")

class ImportScripts::Jan < ImportScripts::Base

  JAN_HOST            ||= ENV['JAN_HOST'] || "localhost"
  JAN_DB              ||= ENV['JAN_DB'] || ""
  BATCH_SIZE          ||= 1000
  JAN_PW              ||= ENV['JAN_PW'] || "password"
  JAN_USER            ||= ENV['JAN_USER'] || "root"
  JAN_PREFIX          ||= ENV['JAN_PREFIX'] || "wp_"
  JAN_ATTACHMENTS_DIR ||= ENV['JAN_ATTACHMENTS_DIR'] || "/path/to/attachments"

  def initialize
    super

    @he = HTMLEntities.new

    @client = Mysql2::Client.new(
      host: JAN_HOST,
      username: JAN_USER,
      database: JAN_DB,
      password: JAN_PW,
    )
  end

  def execute
    import_users
    import_categories
    import_questions
    import_answers
    associate_categories_to_topics
    import_comments_and_staged_users
  end

  def import_users
    puts "", "importing users..."

    last_user_id = -1
    total_users = query(<<-SQL
      SELECT COUNT(DISTINCT(u.id)) AS cnt
      FROM #{JAN_PREFIX}users u
      WHERE user_email LIKE '%@%'
    SQL
    ).first["cnt"]

    batches(BATCH_SIZE) do |offset|
      users = query(<<-SQL
        SELECT u.id, user_nicename, display_name, user_email, user_registered, user_url, user_pass
          FROM #{JAN_PREFIX}users u
         WHERE user_email LIKE '%@%'
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

    categories = query(<<-SQL
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
      category = { id: c['id'], name: c['category_name'], description: c['description'] }
      category
    end
  end

  def import_questions 
    puts "", "importing questions"
    
    last_post_id = -1
    total_posts = query(<<-SQL
    SELECT COUNT(p.ID) count
    FROM wp_posts AS p 
    
    LEFT JOIN wp_postmeta AS m_author_email 
    ON m_author_email.post_id = p.ID 
    AND m_author_email.meta_key = '_dwqa_anonymous_email'
    
    LEFT JOIN wp_postmeta AS m_author 
    ON m_author.post_id = p.ID 
    AND m_author.meta_key = '_dwqa_anonymous_name'
    
    LEFT JOIN wp_postmeta AS m_votes 
    ON m_votes.post_id = p.ID 
    AND m_votes.meta_key = '_dwqa_votes'
    
    LEFT JOIN wp_postmeta 
    AS m_status ON m_status.post_id = p.ID 
    AND m_status.meta_key = '_dwqa_status'
    
    LEFT JOIN wp_postmeta AS m_question 
    ON m_question.post_id = p.ID 
    AND m_question.meta_key = '_question' 
    
    LEFT JOIN wp_postmeta AS m_views 
    ON m_views.post_id = p.ID 
    AND m_views.meta_key = '_dwqa_views'
    
    LEFT JOIN wp_term_relationships AS r 
    ON r.object_id = p.ID
    
    LEFT JOIN wp_term_taxonomy AS tax 
    ON tax.term_taxonomy_id = r.term_taxonomy_id
    
    LEFT JOIN wp_terms AS t 
    ON t.term_id = tax.term_id
    
    WHERE p.post_type = 'dwqa-question' 
    AND taxonomy = 'dwqa-question_category' 
    SQL
    ).first["count"]
    
    batches(BATCH_SIZE) do |offset|
      posts = query(<<-SQL
      SELECT p.ID, 
      p.post_date, 
      p.post_title, 
      p.guid, 
      p.post_type, 
      m_votes.meta_value AS votes, 
      m_status.meta_value AS question_status, 
      m_author.meta_value AS author_name, 
      m_author_email.meta_value AS author_email, 
      m_question.meta_value AS question, 
      m_views.meta_value AS views, 
      p.post_content, t.term_id AS category_id, 
      t.name AS category_name, 
      t.slug AS category_slug 
      FROM wp_posts AS p 

      LEFT JOIN wp_postmeta AS m_author_email 
      ON m_author_email.post_id = p.ID 
      AND m_author_email.meta_key = '_dwqa_anonymous_email'

      LEFT JOIN wp_postmeta AS m_author 
      ON m_author.post_id = p.ID 
      AND m_author.meta_key = '_dwqa_anonymous_name'

      LEFT JOIN wp_postmeta AS m_votes 
      ON m_votes.post_id = p.ID 
      AND m_votes.meta_key = '_dwqa_votes'

      LEFT JOIN wp_postmeta 
      AS m_status ON m_status.post_id = p.ID 
      AND m_status.meta_key = '_dwqa_status'

      LEFT JOIN wp_postmeta AS m_question 
      ON m_question.post_id = p.ID 
      AND m_question.meta_key = '_question' 

      LEFT JOIN wp_postmeta AS m_views 
      ON m_views.post_id = p.ID 
      AND m_views.meta_key = '_dwqa_views'

      LEFT JOIN wp_term_relationships AS r 
      ON r.object_id = p.ID

      LEFT JOIN wp_term_taxonomy AS tax 
      ON tax.term_taxonomy_id = r.term_taxonomy_id

      LEFT JOIN wp_terms AS t 
      ON t.term_id = tax.term_id

      WHERE p.post_type = 'dwqa-question' 
      AND taxonomy = 'dwqa-question_category' 
      AND p.ID > #{last_post_id}
      ORDER BY p.ID
      LIMIT #{BATCH_SIZE}
      SQL
      ).to_a

      break if posts.empty?

      last_post_id = posts[-1]["ID"].to_i
      post_ids = posts.map { |p| p["ID"].to_i }

      next if all_records_exist?(:posts, post_ids)

      post_ids_sql = post_ids.join(",")

      create_posts(posts, total: total_posts, offset: offset) do |p|
        skip = false

        user_id = user_id_from_imported_user_id(p["post_author"]) ||
                  find_user_by_import_id(p["post_author"]).try(:id) ||
                  # user_id_from_imported_user_id(anon_names[p['id']]) ||
                  # find_user_by_import_id(anon_names[p['id']]).try(:id) ||
                  -1

        if user_id == -1
          params = {
            username: p['author_name'],
            email: p['author_email'],
            staged: true
          }
          if p['author_email']
            user = create_user(params, p['author_name'])
            user_id = user[:id]
            p "created a new staged user #{user_id} from question"
          else
            skip = true # skip this entry as its not associated with any user
          end
        end

        post = {
          id: p["ID"],
          user_id: user_id,
          raw: p["post_content"],
          created_at: p["post_date"],
          # like_count: posts_likes[p["id"]],
          topic_id: nil
        }

        if post[:raw].present?
          post[:raw].gsub!(/\<pre\>\<code(=[a-z]*)?\>(.*?)\<\/code\>\<\/pre\>/im) { "```\n#{@he.decode($2)}\n```" }
        end

          post[:category] = category_id_from_imported_category_id(p["post_parent"])
          post[:title] = CGI.unescapeHTML(p["post_title"])

        skip ? nil : post
      end
    end
  end

  def import_answers
    puts "", "importing answers"

    last_post_id = -1
    total_posts = query(<<-SQL
    SELECT COUNT(p.ID) count
    FROM wp_posts AS p

    LEFT JOIN wp_postmeta AS m_author_email 
    ON m_author_email.post_id = p.ID 
    AND m_author_email.meta_key = '_dwqa_anonymous_email'

    LEFT JOIN wp_postmeta AS m_author 
    ON m_author.post_id = p.ID 
    AND m_author.meta_key = '_dwqa_anonymous_name'

    LEFT JOIN wp_postmeta AS m_votes 
    ON m_votes.post_id = p.ID 
    AND m_votes.meta_key = '_dwqa_votes'

    LEFT JOIN wp_posts AS question 
    ON question.id = p.post_parent

    LEFT JOIN wp_users AS u 
    ON u.ID = p.post_author 

    WHERE p.post_type = 'dwqa-answer'
    SQL
    ).first["count"]

    batches(BATCH_SIZE) do |offset|
      posts = query(<<-SQL
      SELECT p.ID,
      p.post_parent, 
      p.post_date, 
      p.post_type, 
      m_votes.meta_value AS votes, 
      IFNULL(m_author.meta_value,u.display_name) AS author_name, 
      IFNULL(m_author_email.meta_value,u.user_email) AS author_email, 
      p.post_content, question.post_title AS question_title 

      FROM wp_posts AS p

      LEFT JOIN wp_postmeta AS m_author_email 
      ON m_author_email.post_id = p.ID 
      AND m_author_email.meta_key = '_dwqa_anonymous_email'

      LEFT JOIN wp_postmeta AS m_author 
      ON m_author.post_id = p.ID 
      AND m_author.meta_key = '_dwqa_anonymous_name'

      LEFT JOIN wp_postmeta AS m_votes 
      ON m_votes.post_id = p.ID 
      AND m_votes.meta_key = '_dwqa_votes'

      LEFT JOIN wp_posts AS question 
      ON question.id = p.post_parent

      LEFT JOIN wp_users AS u 
      ON u.ID = p.post_author 

      WHERE p.post_type = 'dwqa-answer' 
      AND p.ID > #{last_post_id}
      ORDER BY p.ID
      LIMIT #{BATCH_SIZE}
      SQL
      ).to_a

      break if posts.empty?

      last_post_id = posts[-1]["ID"].to_i
      post_ids = posts.map { |p| p["ID"].to_i }

      next if all_records_exist?(:posts, post_ids)

      post_ids_sql = post_ids.join(",")

      create_posts(posts, total: total_posts, offset: offset) do |p|
        skip = false

        user_id = user_id_from_imported_user_id(p["post_author"]) ||
                  find_user_by_import_id(p["post_author"]).try(:id) ||
                  # user_id_from_imported_user_id(anon_names[p['id']]) ||
                  # find_user_by_import_id(anon_names[p['id']]).try(:id) ||
                  -1

        if user_id == -1
          params = {
            username: p['author_name'],
            email: p['author_email'],
            staged: true
          }
          if p['author_email']
            user = create_user(params, p['author_name'])
            user_id = user[:id]
            p "created a new staged user #{user_id} from answer"
          else
            skip = true
          end
        end

        post = {
          id: p["ID"],
          user_id: user_id,
          raw: p["post_content"],
          created_at: p["post_date"],
          # like_count: posts_likes[p["id"]],
        }

        if post[:raw].present?
          post[:raw].gsub!(/\<pre\>\<code(=[a-z]*)?\>(.*?)\<\/code\>\<\/pre\>/im) { "```\n#{@he.decode($2)}\n```" }
        end

        if parent = topic_lookup_from_imported_post_id(p["post_parent"])
          post[:topic_id] = parent[:topic_id]
          post[:reply_to_post_number] = parent[:post_number] if parent[:post_number] > 1
        else
          puts "Skipping #{p["id"]}: #{p["post_content"][0..40]}"
          skip = true
        end


        skip ? nil : post
      end
    end
  end

  def import_comments_and_staged_users
    puts "", "importing comments and anonymous commenters ;) ..."
    comment_id_offset = 50000
    last_comment_id = -1
    total_comments = query(<<-SQL
      SELECT COUNT(*) count
        FROM #{JAN_PREFIX}comments wpc
        LEFT JOIN wp_posts p 
          ON wpc.comment_post_ID = p.ID 
          WHERE p.post_type = 'dwqa-answer' 
          AND p.post_status = 'publish'
          AND wpc.comment_approved = 1
    SQL
    ).first["count"]

    batches(BATCH_SIZE) do |offset|
      comments = query(<<-SQL
        SELECT comment_ID+#{comment_id_offset} as id,
               comment_post_ID,
               comment_author,
               comment_author_email,
               comment_content,
               comment_date,
               user_id
          FROM #{JAN_PREFIX}comments wpc
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
    topic_count = query(<<-SQL
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
      category_assoc = query(<<-SQL
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
      topic = Post.find_by_id(post_id)&.topic # find_by_ doesn't throw execption
      category_id = category_id_from_imported_category_id(row['term_id'])
      next if !topic || !category_id
      topic.category_id = category_id
      topic.save
      p "category #{category_id} updated for topic #{topic.id}"
    end
    end
  end

  def query(sql)
    @client.query(sql, cache_rows: false)
  end

end

ImportScripts::Jan.new.perform
